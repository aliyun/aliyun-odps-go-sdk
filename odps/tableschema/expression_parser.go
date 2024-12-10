package tableschema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
)

// GenerateExpression generates a column expression, used to generate partition values for the Auto-Partition table
type GenerateExpression interface {
	generate(record *data.Record, schema *TableSchema) (string, error)
	String() string
}

func parseGenerateExpression(exprJSON string) (GenerateExpression, error) {
	if len(exprJSON) == 0 {
		return nil, nil
	}
	generateExpression, err := parseFromJSON(exprJSON)
	if err != nil {
		return nil, err
	}

	// 后续遍历，最后一个项目为顶层表达式
	topLevelExpression := generateExpression[len(generateExpression)-1]
	functionCall := topLevelExpression.FunctionCall
	if functionCall != nil && strings.ToLower(functionCall.Name) == "trunc_time" {
		return newTruncTimeWithConstant(
			generateExpression[0].LeafExprDesc.Reference.Name,
			generateExpression[1].LeafExprDesc.Constant,
		)
	}
	return nil, fmt.Errorf("unknown generate expression: %s", getFunctionName(functionCall))
}

type expressionItem struct {
	LeafExprDesc leafExprDesc  `json:"leafExprDesc"`
	FunctionCall *functionCall `json:"functionCall"`
}

type leafExprDesc struct {
	Reference reference `json:"reference"`
	Constant  string    `json:"constant"`
	IsNull    bool      `json:"isNull"`
	Type      string    `json:"type"`
}

type reference struct {
	Name string `json:"name"`
}

type functionCall struct {
	Name           string `json:"name"`
	ParameterCount *int   `json:"parameterCount"`
	Type           string `json:"type"`
}

func parseFromJSON(exprJSON string) ([]expressionItem, error) {
	var expressionItems []expressionItem
	if err := json.Unmarshal([]byte(exprJSON), &expressionItems); err != nil {
		return nil, err
	}
	return expressionItems, nil
}

func getFunctionName(functionCall *functionCall) string {
	if functionCall != nil {
		return functionCall.Name
	}
	return "null"
}
