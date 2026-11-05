package nacosClient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

type INacosClient interface {
}

type NacosClient struct {
	logger     log.Logger
	httpClient http.Client
}

// NewNacosClient returns a client with a bounded cluster-node request timeout.
func NewNacosClient() *NacosClient {
	return &NacosClient{httpClient: http.Client{Timeout: 5 * time.Second}}
}

type ServersInfo struct {
	Servers []ServerInfo `json:"servers"`
}

// ServerInfo preserves the member information used by the operator health check.
type ServerInfo struct {
	IP         string `json:"ip"`
	Port       int    `json:"port"`
	State      string `json:"state"`
	ExtendInfo struct {
		LastRefreshTime int64 `json:"lastRefreshTime"`
		RaftMetaData    struct {
			MetaDataMap struct {
				NamingPersistentService struct {
					Leader          string   `json:"leader"`
					RaftGroupMember []string `json:"raftGroupMember"`
					Term            int      `json:"term"`
				} `json:"naming_persistent_service"`
			} `json:"metaDataMap"`
		} `json:"raftMetaData"`
		RaftPort string `json:"raftPort"`
		Version  string `json:"version"`
	} `json:"extendInfo"`
	Address       string `json:"address"`
	FailAccessCnt int    `json:"failAccessCnt"`
}

func (c *NacosClient) GetClusterNodes(ip string) (ServersInfo, error) {
	endpoint := "http://" + net.JoinHostPort(ip, "8848") + "/nacos/v1/core/cluster/nodes"
	servers := ServersInfo{}
	resp, err := c.httpClient.Get(endpoint)
	if err != nil {
		return servers, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return servers, fmt.Errorf("GET %s: HTTP %d %s", endpoint, resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return servers, fmt.Errorf("GET %s: HTTP %d: read response: %w", endpoint, resp.StatusCode, err)
	}
	var response struct {
		Code *int            `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return servers, fmt.Errorf("GET %s: HTTP %d: decode response: %w", endpoint, resp.StatusCode, err)
	}
	if response.Code == nil {
		return servers, fmt.Errorf("GET %s: HTTP %d: missing or null business code", endpoint, resp.StatusCode)
	}
	if *response.Code != http.StatusOK {
		return servers, fmt.Errorf("GET %s: HTTP %d: business code %d", endpoint, resp.StatusCode, *response.Code)
	}
	data := bytes.TrimSpace(response.Data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return servers, fmt.Errorf("GET %s: HTTP %d, business code %d: missing or null data array", endpoint, resp.StatusCode, *response.Code)
	}
	if err := json.Unmarshal(data, &servers.Servers); err != nil {
		return ServersInfo{}, fmt.Errorf("GET %s: HTTP %d, business code %d: decode data array: %w", endpoint, resp.StatusCode, *response.Code, err)
	}
	return servers, nil
}
