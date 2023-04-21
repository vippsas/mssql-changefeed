package sqltest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/repr"
	"github.com/pkg/errors"
)

type MapRow map[string]interface{}
type MapRows []MapRow

type Row []interface{}
type Rows []Row

func runQuery(dbi interface{}, qry string, args ...interface{}) *sql.Rows {

	switch q := dbi.(type) {
	case CtxQuerier:
		rows, err := q.QueryContext(context.Background(), qry, args...)
		if err != nil {
			panic(errors.WithStack(err))
		}
		return rows
	default:
		panic("unsupported dbi type")
	}

}

func RowIteratorToSlice(rows *sql.Rows) (columns []string, result Rows) {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		panic(errors.WithStack(err))
	}
	types, err := rows.ColumnTypes()
	if err != nil || len(types) != len(columns) {
		panic(errors.WithStack(err))
	}
	n := len(columns)
	rowValues := make([]interface{}, n, n)
	pointers := make([]interface{}, n, n)
	for i := 0; i < len(columns); i++ {
		pointers[i] = &rowValues[i]
	}
	for rows.Next() {
		err = rows.Scan(pointers...)
		if err != nil {
			panic(errors.WithStack(err))
		}

		var row Row

		for i, _ := range columns {
			switch v := rowValues[i].(type) {
			case []uint8:
				row = append(row, fmt.Sprintf("0x%x", v))
			case int64:
				// we don't really know that the query column is int64, that's just how all ints are returned,
				// and it's usually more convenient in tests with int
				row = append(row, int(v))
			case time.Time:
				row = append(row, v)
			default:
				row = append(row, v)
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return
}

func RowsMap(columns []string, rows Rows) (outRows MapRows) {
	for _, row := range rows {
		d := make(MapRow)
		for i, v := range row {
			d[columns[i]] = v
		}
		outRows = append(outRows, d)
	}
	return
}

func DumpRows(rows *sql.Rows) {
	var out bytes.Buffer
	var flags uint = 0 // tabwriter.AlignRight
	writer := tabwriter.NewWriter(&out, 0, 0, 4, ' ', flags)

	columns, parsedRows := RowIteratorToSlice(rows)
	if len(parsedRows) > 0 {
		for _, row := range parsedRows {
			for i, value := range row {
				var val interface{}
				switch v := value.(type) {
				case string:
					val = repr.String(v)
				default:
					val = v
				}
				fmt.Fprintln(writer, fmt.Sprintf("%s\t%v\t", columns[i], val))
			}
			fmt.Fprintln(writer, "----------------\t------------\t")
		}
	}
	writer.Flush()
	fmt.Println(out.String())
}

// Returns the result of a query as a loosely typed structure, for use with test code
func QueryMaps(dbi CtxQuerier, qry string, args ...interface{}) MapRows {
	return RowsMap(RowIteratorToSlice(runQuery(dbi, qry, args...)))
}

func Query(dbi CtxQuerier, qry string, args ...interface{}) Rows {
	_, rows := RowIteratorToSlice(runQuery(dbi, qry, args...))
	// do not use nil but a zero-length slice (for backwards compatability as of this writing, wasn't a conscious decision
	// at the time it was done)
	if len(rows) == 0 {
		rows = Rows{}
	}
	return rows
}

func QueryInt(dbi CtxQuerier, qry string, args ...interface{}) (result int) {
	if err := dbi.QueryRowContext(context.Background(), qry, args...).Scan(&result); err != nil {
		panic(errors.WithStack(err))
	}
	return
}

func QueryString(dbi CtxQuerier, qry string, args ...interface{}) (result string) {
	if err := dbi.QueryRowContext(context.Background(), qry, args...).Scan(&result); err != nil {
		panic(errors.WithStack(err))
	}
	return
}

func QueryTime(dbi CtxQuerier, qry string, args ...interface{}) (result time.Time) {
	if err := dbi.QueryRowContext(context.Background(), qry, args...).Scan(&result); err != nil {
		panic(errors.WithStack(err))
	}
	return
}

// Dump result of query to output
func QueryDump(dbi interface{}, qry string, args ...interface{}) {
	fmt.Println("============================")
	fmt.Println(qry)
	fmt.Println("============================")

	rows := runQuery(dbi, qry, args...)
	DumpRows(rows)
}
