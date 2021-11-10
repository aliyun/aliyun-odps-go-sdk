package odps_test

import (
	"encoding/json"
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"log"
)

func ExampleColumn() {
	raw := `[{
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "name",
            "type": "string"},
           {
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "age",
            "type": "bigint"
           }]`

	var column []odps.Column

	err := json.Unmarshal([]byte(raw), &column)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Printf("%s: %s\n", column[0].Name, column[0].Type)
	fmt.Printf("%s: %s\n", column[1].Name, column[1].Type)

	// Output:
	// name: string
	// age: bigint
}
