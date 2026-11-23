// Licensed under the Apache License, Version 2.0.

package configvalue

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Endpoint validates an HTTP service endpoint. Errors omit the supplied value.
func Endpoint(value, name string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, fmt.Errorf("invalid %s: expected an absolute HTTP(S) URL without credentials, query or fragment", name)
	}
	if port := u.Port(); port != "" {
		if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
			return nil, fmt.Errorf("invalid %s: invalid port", name)
		}
	}
	return u, nil
}

// SQLTunnelRoute validates the mutually exclusive database/sql result routes.
func SQLTunnelRoute(endpoint, quota string) error {
	if endpoint == "" {
		return nil
	}
	if quota != "" {
		return fmt.Errorf("tunnelEndpoint and tunnelQuotaName are mutually exclusive")
	}
	_, err := Endpoint(endpoint, "tunnelEndpoint")
	return err
}

func CanonicalTunnelDSNKey(key string) string {
	switch {
	case strings.EqualFold(key, "tunnelEndpoint"):
		return "tunnelEndpoint"
	case strings.EqualFold(key, "tunnelQuotaName"):
		return "tunnelQuotaName"
	default:
		return key
	}
}

func TimeoutSeconds(value, name string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseUint(value, 10, 31)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: expected a non-negative number of seconds", name)
	}
	return time.Duration(n) * time.Second, nil
}
