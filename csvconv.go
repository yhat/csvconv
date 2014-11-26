package csvconv

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strconv"
)

var (
	errHeaderAlreadySet = errors.New("Header already set")
)

type Reader struct {
	reader    *csv.Reader
	headerSet bool
	header    []string
	nCols     int
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

type colType int

const (
	colTypeNum colType = iota
	colTypeStr
)

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
	for i := range r.header {
		r.header[i] = strconv.Quote(r.header[i])
	}
	r.reader.FieldsPerRecord = r.nCols // only allow n cols from now on
	r.headerSet = true
	return nil
}

type JSONOrient int

const (
	OrientColumns JSONOrient = iota
	OrientRecords
)

// Returns a jsonafiable object
func (r *Reader) toJSONStruct(out io.Writer, orient JSONOrient, nRows int) (int, error) {
	if nRows < 0 {
		nRows = math.MaxInt64
	}
	r.setHeader()
	nRead := 0
	var err error
	switch orient {
	case OrientColumns:
		data := make([][]string, r.nCols)
		for colNum := range data {
			data[colNum] = []string{}
		}
		for rowNum := 0; rowNum < nRows; rowNum++ {
			record, err := r.Read()
			if err != nil {
				// hitting EOF is only an issue if i == 0
				if rowNum == 0 || err != io.EOF {
					return nRead, err
				}
				break
			}
			nRead++
			for colNum := range record {
				data[colNum] = append(data[colNum], record[colNum])
			}
		}
		if _, err = io.WriteString(out, "{"); err != nil {
			return nRead, err
		}
		for colNum := range data {
			headerStr := r.header[colNum] + ":"
			if _, err = io.WriteString(out, headerStr); err != nil {
				return nRead, err
			}
			if _, err = io.WriteString(out, "["); err != nil {
				return nRead, err
			}
			for rowNum := range data[colNum] {
				val := data[colNum][rowNum]
				if _, err = strconv.ParseFloat(val, 64); err != nil {
					if val == "" {
						val = "null"
					} else {
						val = strconv.Quote(val)
					}
				}
				if _, err = io.WriteString(out, val); err != nil {
					return nRead, err
				}
				if rowNum < len(data[colNum])-1 {
					if _, err = io.WriteString(out, ","); err != nil {
						return nRead, err
					}
				}
			}
			if _, err = out.Write([]byte("]")); err != nil {
				return nRead, err
			}
			if colNum < len(data)-1 {
				if _, err = out.Write([]byte(",")); err != nil {
					return nRead, err
				}
			}
		}
		if _, err = out.Write([]byte("}")); err != nil {
			return nRead, err
		}
		return nRead, nil
	case OrientRecords:
		if _, err = io.WriteString(out, "["); err != nil {
			return nRead, err
		}
		for rowNum := 0; rowNum < nRows; rowNum++ {
			record, err := r.Read()
			if err != nil {
				// hitting EOF is only an issue if i == 0
				if rowNum == 0 || err != io.EOF {
					return nRead, err
				}
				break
			}
			if rowNum != 0 {
				if _, err = io.WriteString(out, ","); err != nil {
					return nRead, err
				}
			}
			nRead++
			if _, err = io.WriteString(out, "{"); err != nil {
				return nRead, err
			}
			for colNum, val := range record {
				if colNum != 0 {
					if _, err = io.WriteString(out, ","); err != nil {
						return nRead, err
					}
				}
				if _, err = strconv.ParseFloat(val, 64); err != nil {
					if val == "" {
						val = "null"
					} else {
						val = strconv.Quote(val)
					}
				}
				keyVal := r.header[colNum] + ":" + val
				if _, err = io.WriteString(out, keyVal); err != nil {
					return nRead, err
				}
			}
			if _, err = io.WriteString(out, "}"); err != nil {
				return nRead, err
			}
		}
		if _, err = io.WriteString(out, "]"); err != nil {
			return nRead, err
		}
		return nRead, nil
	default:
		return 0, errors.New("Unknown orient method")
	}
}

// Reads and converts CSV rows to JSON. err will be io.EOF if there where no
// rows read and no more rows to read. Does not return io.EOF if there were less
// than nRows read.
func (r *Reader) ToJSON(orient JSONOrient, nRows int) (rowsRead int, jsonData []byte, err error) {
	buf := bytes.NewBuffer([]byte{})
	rowsRead, err = r.toJSONStruct(buf, orient, nRows)
	if err != nil {
		return rowsRead, []byte{}, err
	}
	jsonData = buf.Bytes()
	return
}

type JSONReader struct {
	headersSet      bool
	expectedHeaders []string
}

func NewJSONReader() *JSONReader {
	return &JSONReader{headersSet: false}
}

func (d *JSONReader) ToCSV(r io.Reader, sep rune) ([]byte, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return []byte{}, err
	}
	byRecord := []map[string]interface{}{}
	if nil == json.Unmarshal(data, &byRecord) {
		return d.parseJSONByRecord(byRecord, sep)
	}
	byColumn := map[string][]interface{}{}
	if nil != json.Unmarshal(data, &byColumn) {
		return []byte{}, errors.New("JSON does not conform to CSV encodings")
	}
	return d.parseJSONByColumn(byColumn)
}

func appendIfMissing(slice []string, i string) []string {
	for _, ele := range slice {
		if ele == i {
			return slice
		}
	}
	return append(slice, i)
}

func (d *JSONReader) parseJSONByRecord(v []map[string]interface{}, sep rune) ([]byte, error) {
	nRows := len(v)
	out := bytes.NewBuffer([]byte{})
	w := csv.NewWriter(out)
	w.Comma = sep
	headers := []string{}
	for _, record := range v {
		for k := range record {
			headers = appendIfMissing(headers, k)
		}
	}
	if !d.headersSet {
		d.headersSet = true
		d.expectedHeaders = headers
	} else {
		contains := func(haystack []string, needle string) bool {
			for _, v := range haystack {
				if needle == v {
					return true
				}
			}
			return false
		}
		for _, h := range headers {
			if !contains(d.expectedHeaders, h) {
				return []byte{}, errors.New("Object did not contain the proper keys")
			}
		}
		headers = d.expectedHeaders
		if err := w.Write(headers); err != nil {
			return []byte{}, err
		}
	}
	nCols := len(headers)
	data := make([][]string, nRows)
	for i := 0; i < nRows; i++ {
		data[i] = make([]string, nCols)
	}
	for rowNum, record := range v {
		for colNum, header := range headers {
			strVal := ""
			val, ok := record[header]
			if ok {
				switch valData := val.(type) {
				case int:
					strVal = fmt.Sprintf("%d", valData)
				case float64:
					strVal = fmt.Sprintf("%f", valData)
				case nil:
					strVal = ""
				case string:
					strVal = fmt.Sprintf("%s", valData)
				default:
					strVal = fmt.Sprintf("%v", valData)
				}
			}
			data[rowNum][colNum] = strVal
		}
	}
	if err := w.WriteAll(data); err != nil {
		return []byte{}, err
	}
	w.Flush()
	return out.Bytes(), nil
}

func (d *JSONReader) parseJSONByColumn(v map[string][]interface{}) ([]byte, error) {
	out := bytes.NewBuffer([]byte{})
	w := csv.NewWriter(out)
	maxLength := 0
	keys := make([]string, 0, len(v))
	colDone := map[string]bool{} // have we read all the values of this col?
	for k, values := range v {
		n := len(values)
		if n > maxLength {
			maxLength = n
		}
		keys = append(keys, k)
		colDone[k] = false
	}
	indexOf := func(slice []string, s string) int {
		for i := 0; i < len(slice); i++ {
			if slice[i] == s {
				return i
			}
		}
		return -1
	}
	if d.headersSet {
		for _, k := range keys {
			if indexOf(d.expectedHeaders, k) < 0 {
				return []byte{}, fmt.Errorf("JSON keys did not match expected headers")
			}
		}
		for _, k := range d.expectedHeaders {
			if _, ok := v[k]; !ok {
				return []byte{}, fmt.Errorf("JSON missing keys")
			}
		}
	} else {
		d.expectedHeaders = keys
		if err := w.Write(keys); err != nil {
			return []byte{}, err
		}
	}
	colnames := d.expectedHeaders
	for i := 0; i < maxLength; i++ {
		row := make([]string, len(colnames))
		for j, col := range colnames {
			values := v[col]
			if len(values) < i {
				row[j] = ""
			} else {
				val := values[i]
				strVal := ""
				switch valData := val.(type) {
				case int:
					strVal = fmt.Sprintf("%d", valData)
				case float64:
					strVal = fmt.Sprintf("%f", valData)
				case nil:
					strVal = ""
				case string:
					strVal = fmt.Sprintf("%s", valData)
				default:
					strVal = fmt.Sprintf("%v", valData)
				}
				row[j] = strVal
			}
		}
		if err := w.Write(row); err != nil {
			return []byte{}, err
		}
	}
	w.Flush()
	return out.Bytes(), nil
}
