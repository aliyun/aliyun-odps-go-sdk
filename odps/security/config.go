// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package security

import (
	"bytes"
	"encoding/xml"
	"net/url"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
)

type Config struct {
	restClient             restclient.RestClient
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
func NewSecurityConfig(restClient restclient.RestClient, withoutExceptionPolicy bool, projectName string) Config {
	return Config{
		restClient:             restClient,
		projectName:            projectName,
		withoutExceptionPolicy: withoutExceptionPolicy,
	}
}

func (sc *Config) BeLoaded() bool {
	return sc.beLoaded
}

func (sc *Config) Load() error {
	rb := common.NewResourceBuilder(sc.projectName)
	resource := rb.Project()
	client := sc.restClient

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

	return errors.WithStack(err)
}

func (sc *Config) Update(supervisionToken string) error {
	rb := common.NewResourceBuilder(sc.projectName)
	resource := rb.Project()
	client := sc.restClient

	queryArgs := make(url.Values, 1)
	queryArgs.Set("security_configuration", "")

	bodyXml, err := xml.Marshal(sc.model)
	if err != nil {
		return errors.WithStack(err)
	}

	req, err := client.NewRequestWithUrlQuery(common.HttpMethod.PutMethod, resource, bytes.NewReader(bodyXml), queryArgs)
	if err != nil {
		return errors.WithStack(err)
	}

	if supervisionToken != "" {
		req.Header.Set(common.HttpHeaderOdpsSupervisionToken, supervisionToken)
	}

	return errors.WithStack(client.DoWithParseFunc(req, nil))
}

func (sc *Config) CheckPermissionUsingAcl() bool {
	return sc.model.CheckPermissionUsingAcl
}

func (sc *Config) EnableCheckPermissionUsingAcl() {
	sc.model.CheckPermissionUsingAcl = true
}

func (sc *Config) DisableCheckPermissionUsingAcl() {
	sc.model.CheckPermissionUsingAcl = false
}

func (sc *Config) CheckPermissionUsingPolicy() bool {
	return sc.model.CheckPermissionUsingPolicy
}

func (sc *Config) EnableCheckPermissionUsingPolicy() {
	sc.model.CheckPermissionUsingPolicy = true
}

func (sc *Config) DisableCheckPermissionUsingPolicy() {
	sc.model.CheckPermissionUsingPolicy = false
}

func (sc *Config) LabelSecurity() bool {
	return sc.model.LabelSecurity
}

func (sc *Config) EnableLabelSecurity() {
	sc.model.LabelSecurity = true
}

func (sc *Config) DisableLabelSecurity() {
	sc.model.LabelSecurity = false
}

func (sc *Config) ObjectCreatorHasAccessPermission() bool {
	return sc.model.ObjectCreatorHasAccessPermission
}

func (sc *Config) EnableObjectCreatorHasAccessPermission() {
	sc.model.ObjectCreatorHasAccessPermission = true
}

func (sc *Config) DisableObjectCreatorHasAccessPermission() {
	sc.model.ObjectCreatorHasAccessPermission = false
}

func (sc *Config) ObjectCreatorHasGrantPermission() bool {
	return sc.model.ObjectCreatorHasGrantPermission
}

func (sc *Config) EnableObjectCreatorHasGrantPermission() {
	sc.model.ObjectCreatorHasGrantPermission = true
}

func (sc *Config) DisableObjectCreatorHasGrantPermission() {
	sc.model.ObjectCreatorHasGrantPermission = false
}

func (sc *Config) ProjectProtection() bool {
	return sc.model.ProjectProtection.Protected == "true"
}

func (sc *Config) EnableProjectProtection() {
	sc.model.ProjectProtection.Protected = "true"
	sc.model.ProjectProtection.Exceptions = ""
}

func (sc *Config) EnableProjectProtectionWithExceptionPolicy(exceptionPolicy string) {
	sc.model.ProjectProtection.Protected = "true"
	sc.model.ProjectProtection.Exceptions = exceptionPolicy
}

func (sc *Config) DisableProjectProtection() {
	sc.model.ProjectProtection.Protected = "true"
	sc.model.ProjectProtection.Exceptions = ""
}

func (sc *Config) ProjectProtectionExceptionPolicy() string {
	return sc.model.ProjectProtection.Exceptions
}

func (sc *Config) CheckPermissionUsingAclV2() bool {
	return sc.model.CheckPermissionUsingAclV2
}

func (sc *Config) CheckPermissionUsingPackageV2() bool {
	return sc.model.CheckPermissionUsingPackageV2
}

func (sc *Config) SupportAcl() bool {
	return sc.model.SupportACL
}

func (sc *Config) SupportPolicy() bool {
	return sc.model.SupportPolicy
}

func (sc *Config) SupportPackage() bool {
	return sc.model.SupportPackage
}

func (sc *Config) SupportAclV2() bool {
	return sc.model.SupportACLV2
}

func (sc *Config) SupportPackageV2() bool {
	return sc.model.SupportPackageV2
}

func (sc *Config) CheckPermissionUsingPackage() bool {
	return sc.model.CheckPermissionUsingPackage
}

func (sc *Config) CreatePackage() bool {
	return sc.model.CreatePackage
}

func (sc *Config) CreatePackageV2() bool {
	return sc.model.CreatePackageV2
}

func (sc *Config) GetAuthorizationVersion() string {
	return sc.model.AuthorizationVersion
}

func (sc *Config) CheckDownloadPrivilege() bool {
	return sc.model.EnableDownloadPrivilege
}

func (sc *Config) EnableDownloadPrivilege() {
	sc.model.EnableDownloadPrivilege = true
}

// DisableDownloadPrivilege If project setting DOWNLOAD_PRIV_ENFORCED is enabled,
// download privilege cannot be set to false via odps sdk
func (sc *Config) DisableDownloadPrivilege() {
	sc.model.EnableDownloadPrivilege = false
}

func (sc *Config) GetGrammarVersion() string {
	return sc.model.GrammarVersion
}
