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

type (
	PermissionObjectType int
	PermissionActionType int
	PermissionEffect     int
)

const (
	_ PermissionObjectType = iota
	ObjectTypeProject
	ObjectTypeTable
	ObjectTypeFunction
	ObjectTypeResource
	ObjectTypeInstance
)

func (p PermissionObjectType) String() string {
	switch p {
	case ObjectTypeProject:
		return "Project"
	case ObjectTypeTable:
		return "Table"
	case ObjectTypeFunction:
		return "Function"
	case ObjectTypeResource:
		return "Resource"
	case ObjectTypeInstance:
		return "Instance"
	default:
		return "UnknownObjectType"
	}
}

const (
	_ PermissionActionType = iota
	ActionTypeRead
	ActionTypeWrite
	ActionTypeList
	ActionTypeCreateTable
	ActionTypeCreateInstance
	ActionTypeCreateFunction
	ActionTypeCreateResource
	ActionTypeAll
	ActionTypeDescribe
	ActionTypeSelect
	ActionTypeAlter
	ActionTypeUpdate
	ActionTypeDrop
	ActionTypeExecute
	ActionTypeDelete
	ActionTypeDownload
)

func (p PermissionActionType) String() string {
	switch p {
	case ActionTypeRead:
		return "Read"
	case ActionTypeWrite:
		return "Write"
	case ActionTypeList:
		return "List"
	case ActionTypeCreateTable:
		return "CreateTable"
	case ActionTypeCreateInstance:
		return "CreateInstance"
	case ActionTypeCreateFunction:
		return "CreateFunction"
	case ActionTypeCreateResource:
		return "CreateResource"
	case ActionTypeAll:
		return "All"
	case ActionTypeDescribe:
		return "Describe"
	case ActionTypeSelect:
		return "Select"
	case ActionTypeAlter:
		return "Alter"
	case ActionTypeUpdate:
		return "Update"
	case ActionTypeDrop:
		return "Drop"
	case ActionTypeExecute:
		return "Execute"
	case ActionTypeDelete:
		return "Delete"
	case ActionTypeDownload:
		return "Download"
	default:
		return "UnknownActionType"
	}
}

const (
	_ PermissionEffect = iota
	EffectAllow
	EffectDeny
)

func (p PermissionEffect) String() string {
	switch p {
	case EffectDeny:
		return "Deny"
	case EffectAllow:
		return "Allow"
	default:
		return "UnknownEffect"
	}
}
