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
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

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
	logViewHost    atomic.Pointer[string]
	jobInsightHost atomic.Pointer[string]
	version        options.LogViewVersion
}

func NewLogView(odpsIns *Odps) *LogView {
	return newLogView(context.Background(), odpsIns)
}

func newLogView(ctx context.Context, odpsIns *Odps) *LogView {
	lv := &LogView{odpsIns: odpsIns, version: odpsIns.Options.LogViewVersion}
	if lv.version == options.Auto {
		lv.version = options.LegacyLogView
		if lv.getJobInsightHost(ctx) != nil {
			lv.version = options.JobInsight
		}
	}
	return lv
}

func (lv *LogView) LogViewHost() string {
	return lv.host(context.Background())
}

func (lv *LogView) host(ctx context.Context) string {
	switch lv.version {
	case options.LegacyLogView:
		if host := lv.resolveHost(ctx, &lv.logViewHost, "/logview/host"); host != nil {
			return *host
		}
		return LogViewHostDefault
	case options.JobInsight:
		if host := lv.getJobInsightHost(ctx); host != nil {
			return *host
		}
		return JobInsightHostDefault
	default:
		return "Unknown LogView Version " + fmt.Sprint(lv.version)
	}
}

func (lv *LogView) getJobInsightHost(ctx context.Context) *string {
	return lv.resolveHost(ctx, &lv.jobInsightHost, "/webconsole/host")
}

func (lv *LogView) resolveHost(ctx context.Context, cache *atomic.Pointer[string], resource string) *string {
	if host := cache.Load(); host != nil {
		return host
	}
	client := lv.odpsIns.RestClient()
	err := client.GetWithParseFuncContext(ctx, resource, nil, nil, func(res *http.Response) error {
		buf, err := io.ReadAll(res.Body)
		if err != nil {
			return errors.WithStack(err)
		}
		if host := strings.TrimSpace(string(buf)); host != "" {
			cache.CompareAndSwap(nil, &host)
		}
		return nil
	})
	if err != nil && resource == "/logview/host" {
		log.Printf("get logview err, %v", err)
	}
	return cache.Load()
}

func (lv *LogView) SetLogViewHost(logViewHost string) {
	lv.logViewHost.Store(&logViewHost)
}

func (lv *LogView) SetJobInsightHost(jobInsightHost string) {
	lv.jobInsightHost.Store(&jobInsightHost)
}

func (lv *LogView) GenerateLogView(instance *Instance, hours int) (string, error) {
	return lv.generate(context.Background(), instance, hours)
}

// GenerateLogViewContext resolves and generates an instance's LogView URL using ctx.
func (odps *Odps) GenerateLogViewContext(ctx context.Context, instance *Instance, hours int) (string, error) {
	return newLogView(ctx, odps).generate(ctx, instance, hours)
}

func (lv *LogView) generate(ctx context.Context, instance *Instance, hours int) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	switch lv.version {
	case options.JobInsight:
		return lv.generateJobInsight(ctx, instance)
	case options.LegacyLogView:
		return lv.generateLogView(ctx, instance, hours)
	default:
		return "", errors.New("Unknown LogView Version " + fmt.Sprint(lv.version))
	}
}

func (lv *LogView) generateLogView(ctx context.Context, instance *Instance, hours int) (string, error) {
	token, err := lv.generateInstanceToken(ctx, instance, hours)
	if err != nil {
		return "", errors.WithStack(err)
	}
	logViewHost := lv.host(ctx)
	if err := ctx.Err(); err != nil {
		return "", err
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

func (lv *LogView) generateJobInsight(ctx context.Context, instance *Instance) (string, error) {
	client := lv.odpsIns.RestClient()
	jobInsightUrl, err := url.Parse(lv.host(ctx))
	if err != nil {
		return "", errors.WithStack(err)
	}

	regionID := lv.odpsIns.regionID(ctx, instance.ProjectName())
	if err := ctx.Err(); err != nil {
		return "", err
	}
	jobInsightUrl.Path = fmt.Sprintf("%s/job-insights", regionID)
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

func (lv *LogView) generateInstanceToken(ctx context.Context, instance *Instance, hours int) (string, error) {
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
	return sm.GenerateAuthorizationTokenContext(ctx, policy)
}
