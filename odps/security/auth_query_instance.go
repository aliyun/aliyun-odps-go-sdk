package security

import (
	"encoding/xml"
	"time"

	"github.com/pkg/errors"
)

const (
	TerminatedStatus = "Terminated"
	FailedStatus     = "Failed"
	RunningStatus    = "Running"
)

type AuthQueryInstance struct {
	result string
	sm     *Manager
	authId string
	isSync bool
}

func newAuthQueryInstance(sm *Manager, authId string) *AuthQueryInstance {
	return &AuthQueryInstance{
		sm:     sm,
		authId: authId,
	}
}

func newAuthQueryInstanceWithResult(result string) *AuthQueryInstance {
	return &AuthQueryInstance{
		result: result,
		isSync: true,
	}
}

func (ai *AuthQueryInstance) WaitForSuccess() (string, error) {
	if ai.isSync {
		return ai.result, nil
	}

	rb := ai.sm.rb()
	authResource := rb.AuthorizationId(ai.authId)
	client := ai.sm.restClient

	type ResModel struct {
		XMLName xml.Name `xml:"AuthorizationQuery"`
		Result  string
		Status  string
	}
	var resModel ResModel

	for {
		err := client.GetWithModel(authResource, nil, &resModel)

		if err != nil {
			return "", err
		}

		if resModel.Status == TerminatedStatus {
			return resModel.Result, nil
		}

		if resModel.Status == FailedStatus {
			return "", errors.Errorf("Authorization query failed: %s", resModel.Result)
		}

		time.Sleep(time.Second * 1)
	}
}
