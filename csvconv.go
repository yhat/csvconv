package csvconv

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
)

type typeDetector struct {
	caster      typeCaster
	nextCasters []typeCaster
}

// The type detector will attempt to cast with the following hierarchy
// 1. int
// 2. float
// 3. string
func newTypeDetector() *typeDetector {
	return &typeDetector{
		caster: castToInt,
		nextCasters: []typeCaster{
			castToFloat,
			castToString,
		},
	}
}

// Have the type detector cast a value with the current caster
func (t *typeDetector) Cast(val string) (interface{}, error) {
	return t.caster(val)
}

// Add an observation to the type caster. This will revert to the next cast
// method if the current one fails
func (t *typeDetector) NewObs(val string) {
	if len(t.nextCasters) == 0 {
		return
	}
	if _, err := t.caster(val); err != nil {
		t.caster, t.nextCasters = t.nextCasters[0], t.nextCasters[1:]
		t.NewObs(val)
	}
	return
}

// take a string value and return a casted type
type typeCaster func(s string) (interface{}, error)

func castToFloat(s string) (interface{}, error) {
	return strconv.ParseFloat(s, 64)
}

func castToInt(s string) (interface{}, error) {
	return strconv.Atoi(s)
}

func castToString(s string) (interface{}, error) {
	return s, nil
}

var (
	errHeaderAlreadySet = errors.New("Header already set")
)

type Reader struct {
	reader    *csv.Reader
	headerSet bool
	header    []string
	nCols     int
	casters   []*typeDetector
}

// Read a record from the data
func (r *Reader) Read() ([]string, error) {
	// if the header has not been set, set it
	if !r.headerSet {
		r.setHeader()
	}
	record, err := r.reader.Read()
	if err != nil {
		return []string{}, err
	}
	if len(record) != len(r.casters) {
		return []string{}, csv.ErrFieldCount
	}
	// show each value to the type casters
	for i, val := range record {
		r.casters[i].NewObs(val)
	}
	return record, nil
}

// Create a csv converter reader
func NewReader(in io.Reader, sep rune) *Reader {
	r := csv.NewReader(in)
	r.Comma = sep
	r.TrimLeadingSpace = true
	return &Reader{
		reader:    r,
		headerSet: false,
		header:    []string{},
	}
}

func (r *Reader) setHeader() error {
	// The first row is always the header try to set it
	if r.headerSet {
		return errHeaderAlreadySet
	}
	record, err := r.reader.Read()
	if err != nil {
		return err
	}
	r.nCols = len(record)
	r.header = record
	r.reader.FieldsPerRecord = r.nCols // only allow n cols from now on
	// initialize casters
	r.casters = make([]*typeDetector, r.nCols)
	for i := range r.casters {
		r.casters[i] = newTypeDetector()
	}
	r.headerSet = true
	return nil
}

type JSONOrient int

const (
	OrientColumns JSONOrient = iota
	OrientRecords
)

// Returns a jsonafiable object
func (r *Reader) toJSONStruct(orient JSONOrient, nRows int) (int, interface{}, error) {
	if nRows < 0 {
		nRows = math.MaxInt64
	}
	r.setHeader()
	nRead := 0
	var err error
	switch orient {
	case OrientColumns:
		data := make([][]interface{}, r.nCols)
		for colNum := range data {
			data[colNum] = []interface{}{}
		}
		for rowNum := 0; rowNum < nRows; rowNum++ {
			record, err := r.Read()
			if err != nil {
				// hitting EOF is only an issue if i == 0
				if rowNum == 0 || err != io.EOF {
					return nRead, nil, err
				}
				break
			}
			nRead++
			for colNum := range record {
				data[colNum] = append(data[colNum], record[colNum])
			}
		}
		for colNum := range data {
			caster := r.casters[colNum].caster
			for rowNum := range data[colNum] {
				s, ok := data[colNum][rowNum].(string)
				if !ok {
					return nRead, nil, errors.New("Type not string")
				}
				data[colNum][rowNum], err = caster(s)
				if err != nil {
					return nRead, nil, err
				}
			}
		}
		mappedData := map[string]interface{}{}
		for colNum, colName := range r.header {
			mappedData[colName] = data[colNum]
		}
		return nRead, mappedData, nil
	case OrientRecords:
		data := make([][]interface{}, 0)
		for rowNum := 0; rowNum < nRows; rowNum++ {
			record, err := r.Read()
			if err != nil {
				// hitting EOF is only an issue if i == 0
				if rowNum == 0 || err != io.EOF {
					return nRead, nil, err
				}
				break
			}
			nRead++
			row := make([]interface{}, len(record))
			for colNum := range record {
				row[colNum] = record[colNum]
			}
			data = append(data, row)
		}
		mappedData := []map[string]interface{}{}
		for rowNum := range data {
			rowMap := map[string]interface{}{}
			for colNum := range data[rowNum] {
				rowMap[r.header[colNum]], err = r.casters[colNum].Cast(data[rowNum][colNum].(string))
				if err != nil {
					return nRead, nil, err
				}
			}
			mappedData = append(mappedData, rowMap)
		}
		return nRead, mappedData, nil
	default:
		return 0, nil, errors.New("Unknown orient method")
	}
}

// Reads and converts CSV rows to JSON. err will be io.EOF if there where no
// rows read and no more rows to read. Does not return io.EOF if there were less
// than nRows read.
func (r *Reader) ToJSON(orient JSONOrient, nRows int) (rowsRead int, jsonData []byte, err error) {
	rowsRead, jsonStruct, err := r.toJSONStruct(orient, nRows)
	if err != nil {
		return rowsRead, []byte{}, err
	}
	jsonData, err = json.Marshal(&jsonStruct)
	return
}
