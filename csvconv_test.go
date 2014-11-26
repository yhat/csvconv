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

type toCSVTest struct {
	output string
	input  string
	sep    rune
	ok     bool
}

var toCSVTests = []toCSVTest{
	toCSVTest{"a,b\n1,2\n4,5\n", `{"a":[1,4],"b":[2,5]}`, ',', true},
	toCSVTest{"a,b\n1,2\n4,\n", `[{"a":1,"b":2},{"a":4,"b":null}]`, ',', true},
}

func TestToCSV(t *testing.T) {
	for _, ct := range toCSVTests {
		r := NewJSONReader()
		out, err := r.ToCSV(strings.NewReader(ct.input), ct.sep)
		if err != nil {
			t.Error(err)
			continue
		}
		outStr := string(out)
		if strings.TrimSpace(outStr) == "" {
			t.Error("got no output from input '%s'", ct.input)
		}
	}
}
