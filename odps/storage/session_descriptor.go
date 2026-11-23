// Licensed under the Apache License, Version 2.0.

package storage

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
)

const descriptorVersion = 1

// ReadSessionKind identifies the data source of a snapshot.
type ReadSessionKind string

const (
	TableSnapshot    ReadSessionKind = "table"
	InstanceSnapshot ReadSessionKind = "instance"
)

// ReadSessionDescriptor is a JSON-serializable reference to a snapshot. Keep
// it with the original endpoint configuration. Treat RouteToken as opaque.
type ReadSessionDescriptor struct {
	Version    int             `json:"version"`
	Kind       ReadSessionKind `json:"kind"`
	ID         string          `json:"id"`
	RouteToken string          `json:"routeToken,omitempty"`
	QuotaName  string          `json:"quotaName,omitempty"`

	Table       odps.TableRef  `json:"table,omitzero"`
	Job         odps.SQLJobRef `json:"job,omitzero"`
	EnableLimit bool           `json:"enableLimit,omitempty"`
	QueryID     *int64         `json:"queryId,omitempty"`
}

func (d ReadSessionDescriptor) session() Session {
	return Session{ID: d.ID, RouteToken: d.RouteToken}
}

func (d ReadSessionDescriptor) clone() ReadSessionDescriptor {
	if d.QueryID != nil {
		queryID := *d.QueryID
		d.QueryID = &queryID
	}
	return d
}

// WriteMode identifies the wire protocol used by a write session.
type WriteMode string

// WriteModeBatch stages streams for a separate commit.
const WriteModeBatch WriteMode = "Batch"

// WriteSessionDescriptor is a JSON-serializable reference to a staged upload.
// Keep it with the endpoint configuration and the intended stream versions.
type WriteSessionDescriptor struct {
	Version    int           `json:"version"`
	Mode       WriteMode     `json:"mode"`
	ID         string        `json:"id"`
	RouteToken string        `json:"routeToken,omitempty"`
	QuotaName  string        `json:"quotaName,omitempty"`
	Table      odps.TableRef `json:"table"`
}

func (d WriteSessionDescriptor) session() Session {
	return Session{ID: d.ID, RouteToken: d.RouteToken}
}

func (c *Client) validateDescriptor(version int, id, project, quota string) error {
	if version != descriptorVersion {
		return fmt.Errorf("storage: unsupported session descriptor version %d", version)
	}
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("storage: session ID is required")
	}
	if project != c.project || quota != c.quota {
		return fmt.Errorf("storage: session descriptor project or quota does not match the client")
	}
	return nil
}
