package odps

import (
	"bytes"
	"encoding/xml"
	"net/url"
)

type SecurityConfig struct {
	odpsIns                *Odps
	projectName            string
	withoutExceptionPolicy bool
	model                  securityConfigModel
	beLoaded               bool
}

type securityConfigModel struct {
	CheckPermissionUsingAcl          bool
	CheckPermissionUsingPolicy       bool
	LabelSecurity                    bool
	ObjectCreatorHasAccessPermission bool
	ObjectCreatorHasGrantPermission  bool
	CheckPermissionUsingAclV2        bool
	CheckPermissionUsingPackageV2    bool
	SupportACL                       bool
	SupportPolicy                    bool
	SupportPackage                   bool
	SupportACLV2                     bool
	SupportPackageV2                 bool
	CheckPermissionUsingPackage      bool
	CreatePackage                    bool
	CreatePackageV2                  bool
	AuthorizationVersion             string
	EnableDownloadPrivilege          bool
	GrammarVersion                   string
	ProjectProtection                struct {
		Protected  string
		Exceptions string
	} `xml:"ProjectProtection"`
}

// NewSecurityConfig withoutExceptionPolicy一般为false
func NewSecurityConfig(odpsIns *Odps, withoutExceptionPolicy bool, projectName ...string) SecurityConfig {
	var _projectName string

	if len(projectName) > 0 {
		_projectName = projectName[0]
	} else {
		_projectName = odpsIns.DefaultProjectName()
	}

	return SecurityConfig{
		odpsIns:                odpsIns,
		projectName:            _projectName,
		withoutExceptionPolicy: withoutExceptionPolicy,
	}
}

func (sc *SecurityConfig) BeLoaded() bool {
	return sc.beLoaded
}

func (sc *SecurityConfig) Load() error {
	rb := NewResourceBuilder(sc.projectName)
	resource := rb.Project()
	client := sc.odpsIns.restClient

	queryArgs := make(url.Values, 1)
	if !sc.withoutExceptionPolicy {
		queryArgs.Set("security_configuration", "")
	} else {
		queryArgs.Set("security_configuration_without_exception_policy", "")
	}

	err := client.GetWithModel(resource, queryArgs, &sc.model)
	if err != nil {
		sc.beLoaded = true
	}

	return err
}

func (sc *SecurityConfig) Update(supervisionToken string) error {
	rb := NewResourceBuilder(sc.projectName)
	resource := rb.Project()
	client := sc.odpsIns.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("security_configuration", "")

	bodyXml, err := xml.Marshal(sc.model)

	if err != nil {
		return err
	}

	req, err := client.NewRequestWithUrlQuery(PutMethod, resource, bytes.NewReader(bodyXml), queryArgs)
	if err != nil {
		return err
	}

	if supervisionToken != "" {
		req.Header.Set(HttpHeaderOdpsSupervisionToken, supervisionToken)
	}

	return client.DoWithParseFunc(req, nil)
}

func (sc *SecurityConfig) CheckPermissionUsingAcl() bool {
	return sc.model.CheckPermissionUsingAcl
}

func (sc *SecurityConfig) EnableCheckPermissionUsingAcl() {
	sc.model.CheckPermissionUsingAcl = true
}

func (sc *SecurityConfig) DisableCheckPermissionUsingAcl() {
	sc.model.CheckPermissionUsingAcl = false
}

func (sc *SecurityConfig) CheckPermissionUsingPolicy() bool {
	return sc.model.CheckPermissionUsingPolicy
}

func (sc *SecurityConfig) EnableCheckPermissionUsingPolicy() {
	sc.model.CheckPermissionUsingPolicy = true
}

func (sc *SecurityConfig) DisableCheckPermissionUsingPolicy() {
	sc.model.CheckPermissionUsingPolicy = false
}

func (sc *SecurityConfig) LabelSecurity() bool {
	return sc.model.LabelSecurity
}

func (sc *SecurityConfig) EnableLabelSecurity() {
	sc.model.LabelSecurity = true
}

func (sc *SecurityConfig) DisableLabelSecurity() {
	sc.model.LabelSecurity = false
}

func (sc *SecurityConfig) ObjectCreatorHasAccessPermission() bool {
	return sc.model.ObjectCreatorHasAccessPermission
}

func (sc *SecurityConfig) EnableObjectCreatorHasAccessPermission() {
	sc.model.ObjectCreatorHasAccessPermission = true
}

func (sc *SecurityConfig) DisableObjectCreatorHasAccessPermission() {
	sc.model.ObjectCreatorHasAccessPermission = false
}

func (sc *SecurityConfig) ObjectCreatorHasGrantPermission() bool {
	return sc.model.ObjectCreatorHasGrantPermission
}

func (sc *SecurityConfig) EnableObjectCreatorHasGrantPermission() {
	sc.model.ObjectCreatorHasGrantPermission = true
}

func (sc *SecurityConfig) DisableObjectCreatorHasGrantPermission() {
	sc.model.ObjectCreatorHasGrantPermission = false
}

func (sc *SecurityConfig) ProjectProtection() bool {
	return sc.model.ProjectProtection.Protected == "true"
}

func (sc *SecurityConfig) EnableProjectProtection() {
	sc.model.ProjectProtection.Protected = "true"
	sc.model.ProjectProtection.Exceptions = ""
}

func (sc *SecurityConfig) EnableProjectProtectionWithExceptionPolicy(exceptionPolicy string) {
	sc.model.ProjectProtection.Protected = "true"
	sc.model.ProjectProtection.Exceptions = exceptionPolicy
}

func (sc *SecurityConfig) DisableProjectProtection() {
	sc.model.ProjectProtection.Protected = "true"
	sc.model.ProjectProtection.Exceptions = ""
}

func (sc *SecurityConfig) ProjectProtectionExceptionPolicy() string {
	return sc.model.ProjectProtection.Exceptions
}

func (sc *SecurityConfig) CheckPermissionUsingAclV2() bool {
	return sc.model.CheckPermissionUsingAclV2
}

func (sc *SecurityConfig) CheckPermissionUsingPackageV2() bool {
	return sc.model.CheckPermissionUsingPackageV2
}

func (sc *SecurityConfig) SupportAcl() bool {
	return sc.model.SupportACL
}

func (sc *SecurityConfig) SupportPolicy() bool {
	return sc.model.SupportPolicy
}

func (sc *SecurityConfig) SupportPackage() bool {
	return sc.model.SupportPackage
}

func (sc *SecurityConfig) SupportAclV2() bool {
	return sc.model.SupportACLV2
}

func (sc *SecurityConfig) SupportPackageV2() bool {
	return sc.model.SupportPackageV2
}

func (sc *SecurityConfig) CheckPermissionUsingPackage() bool {
	return sc.model.CheckPermissionUsingPackage
}

func (sc *SecurityConfig) CreatePackage() bool {
	return sc.model.CreatePackage
}

func (sc *SecurityConfig) CreatePackageV2() bool {
	return sc.model.CreatePackageV2
}

func (sc *SecurityConfig) GetAuthorizationVersion() string {
	return sc.model.AuthorizationVersion
}

func (sc *SecurityConfig) CheckDownloadPrivilege() bool {
	return sc.model.EnableDownloadPrivilege
}

func (sc *SecurityConfig) EnableDownloadPrivilege() {
	sc.model.EnableDownloadPrivilege = true
}

// DisableDownloadPrivilege If project setting DOWNLOAD_PRIV_ENFORCED is enabled,
// download privilege cannot be set to false via odps sdk
func (sc *SecurityConfig) DisableDownloadPrivilege() {
	sc.model.EnableDownloadPrivilege = false
}

func (sc *SecurityConfig) GetGrammarVersion() string {
	return sc.model.GrammarVersion
}
