# csvconv

A Go library for converting CSVs to other encodings and back.

## Usage

```go
package main

import (
	"fmt"
	"strings"

	"github.com/yhat/csvconv"
)

var csvData = `a,b,c
1,2,3
4,5,6`

func main() {
	r := csvconv.NewReader(strings.NewReader(csvData), ',')
	rowsRead, jsonData, err := r.ToJSON(csvconv.OrientColumns, -1)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Read", rowsRead, "rows")
	fmt.Printf("%s\n", jsonData)
}
```

Output:

```
Read 2 rows
{"a":[1,4],"b":[2,5],"c":[3,6]}
```


### TODO:
* JSON to CSV
* Print JSON without using `encoding/json`

