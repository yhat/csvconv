package csvconv

import (
	"fmt"
	"strings"
	"testing"
)

type toJSONTest struct {
	name   string
	input  string
	output string
	sep    rune
	orient JSONOrient
	limit  int
	ok     bool
}

var toJSONTests = []toJSONTest{
	toJSONTest{"null cell", "a,b\n1,2\n4,\n", `{"a":[1,4],"b":[2,null]}`, ',', OrientColumns, -1, true},
	toJSONTest{"null cell", "a,b\n1,2\n4,\n", `[{"a":1,"b":2},{"a":4,"b":null}]`, ',', OrientRecords, -1, true},
	toJSONTest{"bad num cols", "a,b,c\n1,2,3\n4,\n", "", ',', OrientColumns, -1, false},
}

func TestToJSON(t *testing.T) {
	for _, jt := range toJSONTests {
		r := NewReader(strings.NewReader(jt.input), jt.sep)
		_, output, err := r.ToJSON(jt.orient, jt.limit)
		if jt.ok != (err == nil) {
			if jt.ok {
				t.Errorf("%s: did not pass", jt.name)
			} else {
				t.Errorf("%s: expected to fail but passed", jt.name)
			}
		} else {
			if string(output) != jt.output {
				errMsg := fmt.Sprintf(`%s: did not product the correct output
Expected output:
  %s
Resulting output:
  %s`, jt.name, jt.output, string(output))
				t.Error(errMsg)
			}
		}
	}
}
