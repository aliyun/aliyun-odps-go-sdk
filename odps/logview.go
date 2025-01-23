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

package odps

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
)

const (
	HostDefault   = "https://logview.alibaba-inc.com"
	HostDefaultV2 = "https://maxcompute.console.aliyun.com"
)

type LogView struct {
	odpsIns     *Odps
	logViewHost string
}

func NewLogView(odpsIns *Odps) *LogView {
	return &LogView{odpsIns: odpsIns}
}

func (lv *LogView) LogViewHost() string {
	if lv.logViewHost != "" {
		return lv.logViewHost
	}

	client := lv.odpsIns.RestClient()

	err := client.GetWithParseFunc("/logview/host", nil, func(res *http.Response) error {
		// Use ioutil.ReadAll instead of io.ReadAll for compatibility with Go 1.15.
		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return errors.WithStack(err)
		}

		lv.logViewHost = string(buf)

		return nil
	})
	if err != nil {
		log.Printf("get logview err, %v", err)
		return HostDefault
	}

	if lv.logViewHost == "" {
		return HostDefault
	}

	return lv.logViewHost
}

func (lv *LogView) SetLogViewHost(logViewHost string) {
	lv.logViewHost = logViewHost
}

func (lv *LogView) GenerateLogView(instance *Instance, hours int) (string, error) {
	token, err := lv.generateInstanceToken(instance, hours)
	if err != nil {
		return "", errors.WithStack(err)
	}

	logViewHost := lv.LogViewHost()

	logViewUrl, err := url.Parse(logViewHost)
	if err != nil {
		return "", errors.WithStack(err)
	}

	logViewUrl.Path = "logview"
	queryArgs := logViewUrl.Query()
	client := lv.odpsIns.RestClient()
	queryArgs.Set("h", client.Endpoint())
	queryArgs.Set("p", instance.ProjectName())
	queryArgs.Set("i", instance.Id())
	queryArgs.Set("token", token)

	logViewUrl.RawQuery = queryArgs.Encode()
	return logViewUrl.String(), nil
}

func (lv *LogView) GenerateLogViewV2(instance *Instance, regionID string) (string, error) {
	client := lv.odpsIns.RestClient()
	return fmt.Sprintf("%s/%s/job-insights?h=%s&p=%s&i=%s",
		HostDefaultV2,
		regionID,
		client.Endpoint(),
		instance.ProjectName(),
		instance.Id(),
	), nil
}

func (lv *LogView) generateInstanceToken(instance *Instance, hours int) (string, error) {
	policyTpl := `{
	  "expires_in_hours": %d,
	  "policy": {
	      "Statement": [{
	          "Action": ["odps:Read"],
	          "Effect": "Allow",
              "Resource": "acs:odps:*:projects/%s/instances/%s"
	        }],
	        "Version": "1"
      }}`

	policy := fmt.Sprintf(policyTpl, hours, instance.ProjectName(), instance.Id())
	sm := security.NewSecurityManager(lv.odpsIns.RestClient(), instance.ProjectName())
	return sm.GenerateAuthorizationToken(policy)
}
