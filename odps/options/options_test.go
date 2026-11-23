// Licensed under the Apache License, Version 2.0.

package options_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

func TestOdpsOptionDefaultsAndOverrides(t *testing.T) {
	require.Equal(t, &options.OdpsOptions{LogViewVersion: options.Auto, RegionId: "cn"}, options.NewOdpsOptions())
	got := options.NewOdpsOptions(options.WithLogViewVersion(options.JobInsight), options.WithRegionId("cn-hangzhou"))
	require.Equal(t, &options.OdpsOptions{LogViewVersion: options.JobInsight, RegionId: "cn-hangzhou"}, got)
}

func TestSQLTaskOptionsDoNotShareMutableConfiguration(t *testing.T) {
	defaults := options.NewSQLTaskOptions()
	require.Equal(t, "AnonymousSQLTask", defaults.TaskName)
	require.Equal(t, "sql", defaults.Type)
	require.Empty(t, defaults.Hints)
	require.Empty(t, defaults.Aliases)
	require.Nil(t, defaults.InstanceOption)

	hints := map[string]string{"odps.sql.type.system.odps2": "true"}
	aliases := map[string]string{"input": "fixture.source"}
	configured := options.NewSQLTaskOptions(
		options.WithTaskName("fixture_query"),
		options.WithType("cost"),
		options.WithDefaultSchema("analytics"),
		options.WithHints(hints),
		options.WithAliases(aliases),
		options.WithInstanceOption(
			options.WithPriority(4),
			options.WithJobName("fixture_job"),
			options.WithUniqueIdentifyID("fixture-guid"),
			options.WithTryWait(true),
			options.WithMaxQAOptions(
				options.WithMaxQASessionID("explicit-session"),
				options.WithMaxQAQuotaName("interactive"),
				options.WithMaxQAFallback("default", true),
			),
		),
	)
	hints["odps.sql.type.system.odps2"] = "false"
	delete(aliases, "input")

	require.Equal(t, &options.SQLTaskOptions{
		TaskName: "fixture_query", Type: "cost", DefaultSchema: "analytics",
		Hints: map[string]string{"odps.sql.type.system.odps2": "true"}, Aliases: map[string]string{"input": "fixture.source"},
		InstanceOption: &options.CreateInstanceOptions{
			Priority: 4, JobName: "fixture_job", UniqueIdentifyID: "fixture-guid", TryWait: true,
			MaxQAOptions: options.MaxQAOptions{
				UseMaxQA: true, SessionID: "explicit-session", QuotaName: "interactive",
				FallbackInfo: &options.FallbackInfo{FallbackQuota: "default", Fallback: "true"},
			},
		},
	}, configured)
	options.WithMaxQAFallback("other", false)(&configured.InstanceOption.MaxQAOptions)
	require.Equal(t, &options.FallbackInfo{FallbackQuota: "other", Fallback: "false"}, configured.InstanceOption.MaxQAOptions.FallbackInfo)
	require.Empty(t, defaults.Hints)
	require.Empty(t, defaults.Aliases)
}
