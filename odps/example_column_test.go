package odps_test

import (
	"encoding/json"
	"fmt"
	"github.com/aliyun/aliyun-odps-go-sdk/odps"
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
            "type": "string"
           },
           {
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "age",
            "type": "bigint"
           },
           {
            "comment": "",
            "extendedLabels": [],
            "hasDefaultValue": false,
            "isNullable": true,
            "label": "",
            "name": "property",
            "type": "map<varchar(100), struct<a:int, b:int>>"
           }
           ]`

	var column []odps.Column

	err := json.Unmarshal([]byte(raw), &column)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Printf("%s: %s\n", column[0].Name, column[0].Type)
	fmt.Printf("%s: %s\n", column[1].Name, column[1].Type)
	fmt.Printf("%s: %s\n", column[2].Name, column[2].Type)

	// Output:
	// name: STRING
	// age: BIGINT
	// property: MAP<VARCHAR(100),STRUCT<a:INT,b:INT>>
}
