// Licensed under the Apache License, Version 2.0.

package odps

import (
	"context"
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/options"
)

// SQLRequest describes a SQL job submitted in the client's scope.
type SQLRequest struct {
	Statement string
	Options   options.SQLTaskOptions
}

// SQLJobRef identifies a submitted standard SQL job. TaskName can be empty
// when attaching to an instance created outside this SDK.
type SQLJobRef struct {
	Project    string
	InstanceID string
	TaskName   string
}

// SQLJob identifies a submitted SQL job and manages its remote lifecycle.
// Wait, Status and Cancel use separate request contexts.
type SQLJob struct {
	instance Instance
}

// SQLJobError preserves the job reference when an operation fails after a
// submission has been acknowledged. Unwrap exposes the cause.
type SQLJobError struct {
	Job SQLJobRef
	Err error
}

func (e *SQLJobError) Error() string {
	return fmt.Sprintf("SQL job %s/%s: %v", e.Job.Project, e.Job.InstanceID, e.Err)
}

func (e *SQLJobError) Unwrap() error { return e.Err }

// SubmitSQL submits a statement and returns after the server acknowledges it.
// A canceled request can have created a job. When its identity is available,
// a job handle and error are both returned.
func (c *Client) SubmitSQL(ctx context.Context, request SQLRequest) (*SQLJob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.Statement) == "" {
		return nil, fmt.Errorf("SQL statement is required")
	}
	opts := request.Options
	if c.Scope().Schema != "" {
		// A fixed scope supplies defaults. Explicit per-job options take precedence.
		if opts.DefaultSchema == "" {
			opts.DefaultSchema = c.Scope().Schema
		}
	}
	instance, err := c.service.execSQLWithOption(ctx, request.Statement, &opts)
	if instance == nil {
		return nil, err
	}
	job := &SQLJob{instance: *instance}
	return job, job.wrap(err)
}

// ResumeSQL attaches to a standard SQL instance by identity without making a
// request. Use Status or Wait to check the instance remotely.
func (c *Client) ResumeSQL(ref SQLJobRef) (*SQLJob, error) {
	if ref.Project == "" {
		ref.Project = c.Scope().Project
	}
	if err := validateName("project", ref.Project); err != nil {
		return nil, err
	}
	if err := validateName("instance", ref.InstanceID); err != nil {
		return nil, err
	}
	client := c.withScope(Scope{Project: ref.Project, Schema: c.Scope().Schema})
	instance := NewInstance(client.service, ref.Project, ref.InstanceID)
	instance.taskNameCommitted = ref.TaskName
	return &SQLJob{instance: *instance}, nil
}

// Ref returns the job's identity for status, result transfer and recovery.
func (j *SQLJob) Ref() SQLJobRef {
	return SQLJobRef{Project: j.instance.ProjectName(), InstanceID: j.instance.Id(), TaskName: j.instance.TaskNameCommitted()}
}

// Status fetches the current server-side instance status.
func (j *SQLJob) Status(ctx context.Context) (InstanceStatus, error) {
	if err := ctx.Err(); err != nil {
		return 0, j.wrap(err)
	}
	instance := j.instance
	if instance.IsSync() {
		return InstanceTerminated, nil
	}
	if err := instance.load(ctx); err != nil {
		return 0, j.wrap(err)
	}
	return instance.Status(), nil
}

// Wait waits for successful SQL tasks and returns a handle for opening their
// results. Canceling ctx ends the local wait. Cancel requests remote termination.
func (j *SQLJob) Wait(ctx context.Context) (*SQLResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, j.wrap(err)
	}
	instance := j.instance
	if err := instance.WaitForSuccessContext(ctx); err != nil {
		return nil, j.wrap(err)
	}
	return &SQLResult{instance: instance, ref: j.Ref()}, nil
}

// Cancel requests termination of the server-side job.
func (j *SQLJob) Cancel(ctx context.Context) error {
	instance := j.instance
	return j.wrap(instance.TerminateContext(ctx))
}

// LogView resolves and creates a LogView URL for the submitted job.
func (j *SQLJob) LogView(ctx context.Context, hours int) (string, error) {
	instance := j.instance
	link, err := instance.odpsIns.GenerateLogViewContext(ctx, &instance, hours)
	return link, j.wrap(err)
}

func (j *SQLJob) wrap(err error) error {
	if err == nil {
		return nil
	}
	return &SQLJobError{Job: j.Ref(), Err: err}
}

// SQLResult is available after Wait succeeds. Its reference can be used to
// open a Protobuf Tunnel reader or an Arrow Storage read session.
type SQLResult struct {
	instance Instance
	ref      SQLJobRef
}

// Ref returns the identity associated with the result.
func (r *SQLResult) Ref() SQLJobRef { return r.ref }

// Inline returns the small task results stored by the instance.
func (r *SQLResult) Inline(ctx context.Context) ([]TaskResult, error) {
	job := SQLJob{instance: r.instance}
	if err := ctx.Err(); err != nil {
		return nil, job.wrap(err)
	}
	instance := r.instance
	tasks, err := instance.getResult(ctx)
	if err != nil {
		return nil, job.wrap(err)
	}
	return append([]TaskResult(nil), tasks...), nil
}
