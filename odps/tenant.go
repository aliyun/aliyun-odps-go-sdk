package odps

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

// TenantState 租户状态枚举
type TenantState int

const (
	StateNormal TenantState = iota
	StateSuspended
	StateDeleting
	StateDeleted
)

// Tenant 租户实体（对外暴露的接口）
type Tenant struct {
	client *restclient.RestClient
	model  *tenantModel
}

// 私有模型结构体
type tenantModel struct {
	Name             string            `json:"Name"`
	OwnerId          string            `json:"OwnerId"`
	TenantId         string            `json:"TenantId"`
	State            TenantState       `json:"State"`
	CreationTime     common.GMTTime    `json:"CreateTime"`
	LastModifiedTime common.GMTTime    `json:"UpdateTime"`
	TenantMeta       string            `json:"TenantMeta"`
	Parameters       map[string]string `json:"Parameters"`
}

// UnmarshalJSON 自定义TenantState反序列化
func (s *TenantState) UnmarshalJSON(data []byte) error {
	var stateStr string
	if err := json.Unmarshal(data, &stateStr); err != nil {
		return err
	}
	switch strings.ToUpper(stateStr) {
	case "NORMAL":
		*s = StateNormal
	case "SUSPENDED":
		*s = StateSuspended
	case "DELETING":
		*s = StateDeleting
	case "DELETED":
		*s = StateDeleted
	default:
		return errors.New("invalid tenant state: " + stateStr)
	}
	return nil
}

// NewTenant 创建租户实例
func NewTenant(odpsIns *Odps) *Tenant {
	return &Tenant{
		client: &odpsIns.restClient,
		model:  &tenantModel{Parameters: make(map[string]string)},
	}
}

// Load 实现加载
func (t *Tenant) Load() error {
	var tenantResponse struct {
		Tenant *tenantModel `json:"Tenant"`
	}

	err := t.client.GetWithParseFunc("/tenants", nil, nil, func(res *http.Response) error {
		decoder := json.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&tenantResponse))
	})
	if err != nil {
		return err
	}

	t.model = tenantResponse.Tenant
	return nil
}

// GetProperty 获取租户属性
func (t *Tenant) GetProperty(key string) string {
	return t.model.Parameters[key]
}

// GetName 获取租户名称
func (t *Tenant) GetName() string {
	return t.model.Name
}

// GetOwnerId 获取所有者ID
func (t *Tenant) GetOwnerId() string {
	return t.model.OwnerId
}

// GetTenantId 获取租户ID
func (t *Tenant) GetTenantId() string {
	return t.model.TenantId
}

// GetState 获取租户状态
func (t *Tenant) GetState() TenantState {
	return t.model.State
}

// GetCreationTime 获取创建时间
func (t *Tenant) GetCreationTime() time.Time {
	return time.Time(t.model.CreationTime)
}

// GetLastModifiedTime 获取最后修改时间
func (t *Tenant) GetLastModifiedTime() time.Time {
	return time.Time(t.model.LastModifiedTime)
}

const tenantIDPlaceHolder = "_empty_tenant_"

// GetTenantRolePolicy 获取角色策略
func (t *Tenant) GetTenantRolePolicy(roleName string) (string, error) {
	if roleName == "" {
		return "", errors.New("roleName cannot be empty")
	}

	resource := fmt.Sprintf("/tenants/%s/authorization/roles/%s", tenantIDPlaceHolder, roleName)
	params := url.Values{}
	params.Set("policy", "")

	var response struct {
		Policy string `json:"Policy"`
	}

	err := t.client.GetWithParseFunc(resource, params, nil, func(res *http.Response) error {
		decoder := json.NewDecoder(res.Body)
		return errors.WithStack(decoder.Decode(&response))
	})
	if err != nil {
		return "", errors.Wrap(err, "get tenant role policy failed.")
	}
	return response.Policy, nil
}

// PutTenantRolePolicy 设置角色策略
func (t *Tenant) PutTenantRolePolicy(roleName, policy string) error {
	if roleName == "" {
		return errors.New("roleName cannot be empty")
	}
	if policy == "" {
		return errors.New("policy cannot be empty")
	}

	resource := fmt.Sprintf("/tenants/%s/authorization/roles/%s", tenantIDPlaceHolder, roleName)
	params := url.Values{}
	params.Set("policy", "")

	return t.client.PutWithParseFunc(resource, params,
		bytes.NewReader([]byte(policy)), nil)
}
