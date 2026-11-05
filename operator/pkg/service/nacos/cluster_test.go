package nacosClient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const clusterNodesPath = "/nacos/v1/core/cluster/nodes"

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func responseClient(status int, body string, requestURL *string) *NacosClient {
	return &NacosClient{httpClient: http.Client{
		Timeout: 5 * time.Second,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if requestURL != nil {
				*requestURL = req.URL.String()
			}
			return &http.Response{
				StatusCode: status,
				Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		}),
	}}
}

func TestGetClusterNodesParsesCurrentAPI(t *testing.T) {
	const leader = "nacos-0.nacos-headless.base.svc.cluster.local:7848"
	node := `{"ip":"192.0.2.10","port":8848,"state":"UP","extendInfo":{"lastRefreshTime":123,"raftMetaData":{"metaDataMap":{"naming_persistent_service":{"leader":"` + leader + `","raftGroupMember":["nacos-0","nacos-1","nacos-2","nacos-3"],"term":3}}},"raftPort":"7848","version":"2.4.3"},"address":"192.0.2.10:8848","failAccessCnt":0}`
	body := `{"code":200,"message":null,"data":[` + strings.Join([]string{node, node, node, node}, ",") + `]}`
	var requestedURL string
	got, err := responseClient(http.StatusOK, body, &requestedURL).GetClusterNodes("192.0.2.10")
	if err != nil {
		t.Fatal(err)
	}
	if requestedURL != "http://192.0.2.10:8848"+clusterNodesPath {
		t.Fatalf("unexpected endpoint: %s", requestedURL)
	}
	if len(got.Servers) != 4 {
		t.Fatalf("expected four members, got %d", len(got.Servers))
	}
	for _, member := range got.Servers {
		if member.State != "UP" || member.IP != "192.0.2.10" || member.Port != 8848 || member.Address != "192.0.2.10:8848" || member.ExtendInfo.Version != "2.4.3" || member.ExtendInfo.RaftMetaData.MetaDataMap.NamingPersistentService.Leader != leader || len(member.ExtendInfo.RaftMetaData.MetaDataMap.NamingPersistentService.RaftGroupMember) != 4 {
			t.Fatalf("member fields were not preserved: %+v", member)
		}
	}
}

func TestGetClusterNodesRejectsHTTPFailures(t *testing.T) {
	for _, status := range []int{http.StatusNotImplemented, http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			const bodyMarker = "private-response-marker"
			body := fmt.Sprintf(`{"status":%d,"message":"%s","path":"/nacos/v1/ns/operator/servers"}`, status, bodyMarker)
			got, err := responseClient(status, body, nil).GetClusterNodes("192.0.2.10")
			if err == nil {
				t.Fatalf("HTTP %d was accepted as %d members", status, len(got.Servers))
			}
			if !strings.Contains(err.Error(), "http://192.0.2.10:8848"+clusterNodesPath) || !strings.Contains(err.Error(), fmt.Sprint(status)) {
				t.Fatalf("missing endpoint/status in error: %v", err)
			}
			if strings.Contains(err.Error(), bodyMarker) {
				t.Fatalf("error exposed response body: %v", err)
			}
		})
	}
}

func TestGetClusterNodesRejectsInvalidEnvelope(t *testing.T) {
	cases := []struct{ name, body, errorPart string }{
		{"business_failure", `{"code":503,"message":"unavailable","data":[]}`, "business code 503"},
		{"bad_json", `{"code":200,"data":[`, "decode"},
		{"missing_code", `{"data":[]}`, "code"},
		{"null_code", `{"code":null,"data":[]}`, "code"},
		{"missing_data", `{"code":200}`, "data"},
		{"null_data", `{"code":200,"data":null}`, "data"},
		{"object_data", `{"code":200,"data":{}}`, "data"},
		{"string_data", `{"code":200,"data":"not-an-array"}`, "data"},
		{"old_servers_envelope", `{"servers":[{"state":"UP"}]}`, "code"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := responseClient(http.StatusOK, tc.body, nil).GetClusterNodes("192.0.2.10")
			if err == nil {
				t.Fatalf("invalid response accepted as %d members: %s", len(got.Servers), tc.body)
			}
			for _, part := range []string{"http://192.0.2.10:8848" + clusterNodesPath, "HTTP 200", tc.errorPart} {
				if !strings.Contains(err.Error(), part) {
					t.Fatalf("missing %q in error: %v", part, err)
				}
			}
		})
	}
}

func TestGetClusterNodesAcceptsEmptyData(t *testing.T) {
	got, err := responseClient(http.StatusOK, `{"code":200,"data":[]}`, nil).GetClusterNodes("192.0.2.10")
	if err != nil {
		t.Fatal(err)
	}
	if got.Servers == nil || len(got.Servers) != 0 {
		t.Fatalf("expected a valid empty member array, got %#v", got.Servers)
	}
}

func TestGetClusterNodesIPv6Endpoint(t *testing.T) {
	var requestedURL string
	_, err := responseClient(http.StatusOK, `{"code":200,"data":[]}`, &requestedURL).GetClusterNodes("2001:db8::1")
	if err != nil {
		t.Fatal(err)
	}
	if requestedURL != "http://[2001:db8::1]:8848"+clusterNodesPath {
		t.Fatalf("unexpected IPv6 endpoint: %s", requestedURL)
	}
}

func TestGetClusterNodesTimeout(t *testing.T) {
	c := &NacosClient{httpClient: http.Client{
		Timeout: 20 * time.Millisecond,
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}}
	_, err := c.GetClusterNodes("192.0.2.10")
	if err == nil {
		t.Fatal("expected timeout")
	}
	var networkError net.Error
	if !errors.As(err, &networkError) || !networkError.Timeout() {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error was not preserved: %v", err)
	}
	if !strings.Contains(err.Error(), "http://192.0.2.10:8848"+clusterNodesPath) {
		t.Fatalf("missing endpoint in timeout: %v", err)
	}
}

func TestNewNacosClientHasBoundedTimeout(t *testing.T) {
	c := NewNacosClient()
	if c == nil {
		t.Fatal("expected a client")
	}
	if c.httpClient.Timeout != 5*time.Second {
		t.Fatalf("expected a 5-second timeout, got %s", c.httpClient.Timeout)
	}
}
