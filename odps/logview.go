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
	"strings"

	"github.com/pkg/errors"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/security"
)

const (
	LogViewHostDefault    = "https://logview.alibaba-inc.com"
	JobInsightHostDefault = "https://maxcompute.console.aliyun.com"
)

type LogView struct {
	odpsIns        *Odps
	logViewHost    *string
	jobInsightHost *string
	version        int
}

func NewLogView(odpsIns *Odps) *LogView {
	var version int
	if odpsIns.Options.LogViewVersion == options.Auto {
		tempIns := &LogView{odpsIns: odpsIns}
		jobInsightHost := tempIns.getJobInsightHost()
		if jobInsightHost == nil {
			version = 1
		} else {
			version = 2
		}
	} else {
		version = int(odpsIns.Options.LogViewVersion)
	}
	return &LogView{odpsIns: odpsIns, version: version}
}

func (lv *LogView) LogViewHost() string {
	if lv.version == 1 {
		logViewHost := lv.getLogViewHost()
		if logViewHost == nil {
			return LogViewHostDefault
		} else {
			return *logViewHost
		}
	} else if lv.version == 2 {
		jobInsightHost := lv.getJobInsightHost()
		if jobInsightHost == nil {
			return JobInsightHostDefault
		} else {
			return *jobInsightHost
		}
	} else {
		return "Unknown LogView Version " + fmt.Sprint(lv.version)
	}
}

func (lv *LogView) getLogViewHost() *string {
	if lv.logViewHost != nil {
		return lv.logViewHost
	}
	client := lv.odpsIns.RestClient()
	err := client.GetWithParseFunc("/logview/host", nil, nil, func(res *http.Response) error {
		// Use ioutil.ReadAll instead of io.ReadAll for compatibility with Go 1.15.
		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return errors.WithStack(err)
		}
		result := string(buf)
		if strings.TrimSpace(result) == "" {
			return nil
		}
		lv.logViewHost = &result
		return nil
	})
	if err != nil {
		log.Printf("get logview err, %v", err)
		return nil
	}
	return lv.logViewHost
}

func (lv *LogView) getJobInsightHost() *string {
	if lv.jobInsightHost != nil {
		return lv.jobInsightHost
	}
	client := lv.odpsIns.RestClient()
	err := client.GetWithParseFunc("/webconsole/host", nil, nil, func(res *http.Response) error {
		// Use ioutil.ReadAll instead of io.ReadAll for compatibility with Go 1.15.
		buf, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return errors.WithStack(err)
		}
		result := string(buf)
		if strings.TrimSpace(result) == "" {
			return nil
		}
		lv.jobInsightHost = &result
		return nil
	})
	if err != nil {
		return nil
	}
	return lv.jobInsightHost
}

func (lv *LogView) SetLogViewHost(logViewHost string) {
	lv.logViewHost = &logViewHost
}

func (lv *LogView) SetJobInsightHost(jobInsightHost string) {
	lv.jobInsightHost = &jobInsightHost
}

func (lv *LogView) GenerateLogView(instance *Instance, hours int) (string, error) {
	if lv.version == 2 {
		return lv.generateJobInsight(instance)
	} else if lv.version == 1 {
		return lv.generateLogView(instance, hours)
	} else {
		return "", errors.New("Unknown LogView Version " + fmt.Sprint(lv.version))
	}
}

func (lv *LogView) generateLogView(instance *Instance, hours int) (string, error) {
	token, err := lv.generateInstanceToken(instance, hours)
	if err != nil {
		return "", errors.WithStack(err)
	}
	var logViewHost string
	if lv.getLogViewHost() == nil {
		logViewHost = LogViewHostDefault
	} else {
		logViewHost = *lv.getLogViewHost()
	}
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

func (lv *LogView) generateJobInsight(instance *Instance) (string, error) {
	client := lv.odpsIns.RestClient()
	var jobInsightHost string
	if lv.getJobInsightHost() == nil {
		jobInsightHost = JobInsightHostDefault
	} else {
		jobInsightHost = *lv.getJobInsightHost()
	}

	jobInsightUrl, err := url.Parse(jobInsightHost)
	if err != nil {
		return "", errors.WithStack(err)
	}

	jobInsightUrl.Path = fmt.Sprintf("%s/job-insights", lv.odpsIns.RegionId())
	queryArgs := jobInsightUrl.Query()
	queryArgs.Set("h", client.Endpoint())
	queryArgs.Set("p", instance.ProjectName())
	queryArgs.Set("i", instance.Id())

	jobInsightUrl.RawQuery = queryArgs.Encode()
	return jobInsightUrl.String(), nil
}

// Deprecated: As of Go SDK 0.4.9, this function simply calls [odps.GenerateLogView].
func (lv *LogView) GenerateLogViewV2(instance *Instance, regionID string) (string, error) {
	client := lv.odpsIns.RestClient()
	return fmt.Sprintf("%s/%s/job-insights?h=%s&p=%s&i=%s",
		JobInsightHostDefault,
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
