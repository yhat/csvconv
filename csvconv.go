package csvconv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"io"
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
			if _, err = out.Write([]byte("[")); err != nil {
				return nRead, err
			}
			for rowNum := range data[colNum] {
				val := data[colNum][rowNum]
				if _, err = strconv.ParseFloat(val, 64); err != nil {
					val = strconv.Quote(val)
				}
				if _, err = out.Write([]byte(val)); err != nil {
					return nRead, err
				}
				if rowNum < len(data[colNum])-1 {
					if _, err = out.Write([]byte(",")); err != nil {
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
					val = strconv.Quote(val)
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
