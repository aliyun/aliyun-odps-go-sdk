package odps

import (
	"encoding/xml"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TODO 将status转换为enum

type ProjectStatus int

const (
	_ = iota

	ProjectStatusAvailable
	ProjectStatusReadOnly
	ProjectStatusDeleting
	ProjectStatusFrozen
	ProjectStatusUnKnown
)

const (
	// ProjectTypeManaged ordinary project
	ProjectTypeManaged = "managed"
	// ProjectExternalExternal external project，like hive
	ProjectExternalExternal = "external"
)

type Project struct {
	model         projectModel
	allProperties []common.Property
	exists        bool
	beLoaded      bool
	odpsIns       *Odps
	rb            common.ResourceBuilder
}

func (p *Project) OdpsIns() *Odps {
	return p.odpsIns
}

type projectModel struct {
	XMLName            xml.Name          `xml:"Project"`
	Name               string            `xml:"Name"`
	Type               string            `xml:"Type"`
	Comment            string            `xml:"Comment"`
	Status             ProjectStatus     `xml:"State"`
	ProjectGroupName   string            `xml:"ProjectGroupName"`
	Properties         []common.Property `xml:"Properties>Property"`
	DefaultCluster     string            `xml:"DefaultCluster"`
	Clusters           []Cluster         `xml:"Clusters"`
	ExtendedProperties []common.Property `xml:"ExtendedProperties>Property"`
	// 这三个字段在/projects中和/projects/<ProjectName>接口中返回的未知不一样,
	// 前者是body的xml数据中，后者在header里
	Owner            string         `xml:"Owner"`
	CreationTime     common.GMTTime `xml:"CreationTime"`
	LastModifiedTime common.GMTTime `xml:"LastModifiedTime"`
}

type OptionalQuota struct {
	XMLName    xml.Name          `xml:"OptionalQuota"`
	QuotaId    string            `xml:"QuotaID"`
	Properties common.Properties `xml:"Properties"`
}

type Cluster struct {
	Name    string          `xml:"Name"`
	QuotaId string          `xml:"QuotaId"`
	Quotas  []OptionalQuota `xml:"Quotas"`
}

func NewProject(name string, odpsIns *Odps) Project {
	return Project{
		model:   projectModel{Name: name},
		odpsIns: odpsIns,
		rb:      common.ResourceBuilder{ProjectName: name},
	}
}

func (p *Project) RestClient() restclient.RestClient {
	return p.odpsIns.restClient
}

type optionalParams struct {
	// For compatibility. The static class 'Cluster' had strict schema validation. Unmarshalling will
	// fail because of the new xml tag 'Quotas'.
	usedByGroupApi     bool
	withAllProperties  bool
	extendedProperties bool
}

func (p *Project) _loadFromOdps(params optionalParams) (*projectModel, error) {
	resource := p.rb.Project()
	client := p.RestClient()

	var urlQuery = make(url.Values)

	if params.usedByGroupApi {
		urlQuery.Set("isGroupApi", "true")
	}

	if params.withAllProperties {
		urlQuery.Set("properties", "all")
	}

	if params.extendedProperties {
		urlQuery.Set("extended", "")
	}

	model := projectModel{}

	parseFunc := func(res *http.Response) error {
		decoder := xml.NewDecoder(res.Body)
		if err := decoder.Decode(&model); err != nil {
			return errors.WithStack(err)
		}

		header := res.Header
		model.Owner = header.Get(common.HttpHeaderOdpsOwner)

		creationTime, err := common.ParseRFC1123Date(header.Get(common.HttpHeaderOdpsCreationTime))
		if err != nil {
			log.Printf("/project get creation time error, %v", err)
		}

		lastModifiedTime, _ := common.ParseRFC1123Date(header.Get(common.HttpHeaderLastModified))
		if err != nil {
			log.Printf("/project get last modified time error, %v", err)
		}

		model.CreationTime = common.GMTTime(creationTime)
		model.LastModifiedTime = common.GMTTime(lastModifiedTime)

		return nil
	}

	if err := client.GetWithParseFunc(resource, urlQuery, parseFunc); err != nil {
		return nil, errors.WithStack(err)
	}

	return &model, nil
}

// Load should be called before get properties of project
func (p *Project) Load() error {
	model, err := p._loadFromOdps(optionalParams{})
	p.beLoaded = true

	if err != nil {
		if httpNoteOk, ok := err.(restclient.HttpNotOk); ok {
			if httpNoteOk.StatusCode == 404 {
				p.exists = false
			}
		}

		return errors.WithStack(err)
	}

	p.exists = true
	p.model = *model
	return nil
}

// IsLoaded whether `Load()` has been called
func (p *Project) IsLoaded() bool {
	return p.beLoaded
}

func (p *Project) Name() string {
	return p.model.Name
}

func (p *Project) Type() string {
	return p.model.Type
}

func (p *Project) Comment() string {
	return p.model.Comment
}

func (p *Project) Status() ProjectStatus {
	return p.model.Status
}

func (p *Project) ProjectGroupName() string {
	return p.model.ProjectGroupName
}

// PropertiesHasBeSet Properties get the properties those have be set for the project
func (p *Project) PropertiesHasBeSet() common.Properties {
	return p.model.Properties
}

// GetAllProperties get all the configurable properties of the project, including the
// properties inherit from group.
// **note**, this method may return error when something wrong during loading data
// from the api serer
func (p *Project) GetAllProperties() (common.Properties, error) {
	if p.allProperties != nil {
		return p.allProperties, nil
	}

	model, err := p._loadFromOdps(optionalParams{withAllProperties: true})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p.allProperties = model.Properties

	return p.allProperties, nil
}

// GetDefaultCluster Get default cluster. This is an internal method for group-api.
// Returns efault cluster when called by group owner, otherwise ,null.
// **note**, this method may return error when something wrong during loading data
// from the api serer
func (p *Project) GetDefaultCluster() (string, error) {
	if p.model.DefaultCluster != "" {
		return p.model.DefaultCluster, nil
	}

	model, err := p._loadFromOdps(optionalParams{usedByGroupApi: true})
	if err != nil {
		return "", errors.WithStack(err)
	}

	p.model.DefaultCluster = model.DefaultCluster
	p.model.Clusters = model.Clusters

	return p.model.DefaultCluster, nil
}

// GetClusters Get information of clusters owned by this project. This is an internal
// method for group-api.
// **note**, this method may return error when something wrong during loading data
// from the api serer
func (p *Project) GetClusters() ([]Cluster, error) {
	if p.model.Clusters != nil {
		return p.model.Clusters, nil
	}

	model, err := p._loadFromOdps(optionalParams{usedByGroupApi: true})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p.model.DefaultCluster = model.DefaultCluster
	p.model.Clusters = model.Clusters

	return p.model.Clusters, nil
}

// GetExtendedProperties get the extended properties of the project
// **note**, this method may return error when something wrong during loading data
// from the api serer
func (p *Project) GetExtendedProperties() (common.Properties, error) {
	if p.model.ExtendedProperties != nil {
		return p.model.ExtendedProperties, nil
	}

	model, err := p._loadFromOdps(optionalParams{extendedProperties: true})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p.model.ExtendedProperties = model.ExtendedProperties

	return p.model.ExtendedProperties, nil
}

func (p *Project) Owner() string {
	return p.model.Owner
}

func (p *Project) CreationTime() time.Time {
	return time.Time(p.model.CreationTime)
}

func (p *Project) LastModifiedTime() time.Time {
	return time.Time(p.model.LastModifiedTime)
}

func (p *Project) Existed() bool {
	return p.exists
}

func (p *Project) SecurityManager() security.Manager {
	return security.NewSecurityManager(p.odpsIns.restClient, p.Name())
}

func (p *Project) GetTunnelEndpoint() (string, error) {
	client := p.odpsIns.restClient
	resource := p.rb.Tunnel()
	queryArgs := make(url.Values, 1)
	queryArgs.Set("service", "")
	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.GetMethod, resource, nil, queryArgs)
	if err != nil {
		return "", errors.WithStack(err)
	}

	schema := req.URL.Scheme
	var tunnelEndpoint string
	err = client.DoWithParseFunc(req, func(res *http.Response) error {
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return errors.WithStack(err)
		}
		tunnelEndpoint = string(b)
		return nil
	})

	return fmt.Sprintf("%s://%s", schema, tunnelEndpoint), errors.WithStack(err)
}

// Update the project properties, the properties are different in different versioned odps.
// When the "properties" is nil, the system will give the project all the default properties.
// You'd better ask technique support help when using this method.
func (p *Project) Update(properties map[string]string) error {
	type BodyModel struct {
		Name       string
		Properties common.Properties
	}

	_properties := make(common.Properties, 0, len(properties))
	for key, value := range properties {
		_properties = append(_properties, common.Property{Name: key, Value: value})
	}

	var bodyModel = BodyModel{
		Name:       p.Name(),
		Properties: _properties,
	}

	resource := p.rb.Project()
	client := p.RestClient()
	return client.DoXmlWithModel(common.HttpMethod.PutMethod, resource, nil, &bodyModel, nil)
}

func (status *ProjectStatus) FromStr(s string) {
	switch strings.ToUpper(s) {
	case "AVAILABLE":
		*status = ProjectStatusAvailable
	case "READONLY":
		*status = ProjectStatusReadOnly
	case "DELETING":
		*status = ProjectStatusDeleting
	case "FROZEN":
		*status = ProjectStatusFrozen
	default:
		*status = ProjectStatusUnKnown
	}
}

func (status ProjectStatus) String() string {
	switch status {
	case ProjectStatusAvailable:
		return "AVAILABLE"
	case ProjectStatusReadOnly:
		return "READONLY"
	case ProjectStatusDeleting:
		return "DELETING"
	case ProjectStatusFrozen:
		return "FROZEN"
	default:
		return "UNKNOWN"
	}
}

func (status *ProjectStatus) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var s string

	if err := d.DecodeElement(&s, &start); err != nil {
		return errors.WithStack(err)
	}

	status.FromStr(s)

	return nil
}

func (status ProjectStatus) MarshalXML(d *xml.Encoder, start xml.StartElement) error {
	s := status.String()
	return errors.WithStack(d.EncodeElement(s, start))
}
