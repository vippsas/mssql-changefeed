package sqltest

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
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

func QueryT[T any](dbi CtxQuerier, qry string, args ...interface{}) (result T) {
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

func StructSlice2[T any](ctx context.Context, querier CtxQuerier, qry string, args ...interface{}) (result []T, err error) {
	var x T
	err = Structs(ctx, querier, &x, func() error {
		result = append(result, x)
		return nil
	}, qry, args...)
	return
}
func Structs(ctx context.Context, querier CtxQuerier, pointerToStruct interface{}, next func() error, qry string, args ...interface{}) error {
	// Avoid nil pointer errors and invalid API use
	if querier == nil {
		return errors.Errorf("querier cannot be nil")
	}
	if pointerToStruct == nil {
		return errors.Errorf("pointerToStruct cannot be nil!")
	}
	if reflect.TypeOf(pointerToStruct).Kind() != reflect.Ptr {
		return errors.Errorf("Expecting pointerToStruct to be a pointer to a struct, did you forget a '&'?")
	}
	if reflect.ValueOf(pointerToStruct).IsNil() {
		return errors.Errorf("pointerToStruct cannot be nil!")
	}

	rows, err := querier.QueryContext(ctx, qry, args...)
	if err != nil {
		return errors.WithStack(err)
	}

	// Closing rows is critical to return db connection to pool
	defer rows.Close()

	ptrs, err := GetPointersToFields(rows, pointerToStruct)
	if err != nil {
		return err
	}

	// Scan each row and call next
	for rows.Next() {
		// Scan a row into the reordered pointers
		if err := rows.Scan(ptrs...); err != nil {
			return errors.WithStack(err)
		}
		// Call next closure to tell our caller to consume the single struct value
		if err := next(); err != nil {
			return errors.WithStack(err)
		}
	}

	// This final error check is important, and easily forgotten
	if err := rows.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func GetPointersToFields(rows *sql.Rows, pointerToStruct interface{}) ([]interface{}, error) {
	// Gets the names of columns in the query
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for i, name := range columns {
		columns[i] = canonicalName(name)
	}

	// Get the names of struct fields, recursing into embedded structs
	names := DeepFieldNames(pointerToStruct)
	for i, name := range names {
		names[i] = canonicalName(name)
	}

	// Build a mapping from name to index, this index is
	// both for names[i] and origPtrs[i]
	name2index := make(map[string]int, len(names))
	for i, name := range names {
		name2index[name] = i
	}

	// Get pointers in ordering determined by struct
	origPtrs := DeepFieldPointers(pointerToStruct)

	// Reorder pointers to match query column order
	ptrs := make([]interface{}, 0, len(columns))
	mappedNames := make([]string, 0, len(columns))
	n := 0
	for _, col := range columns {
		if j, ok := name2index[col]; ok {
			ptrs = append(ptrs, origPtrs[j])
			mappedNames = append(mappedNames, names[j])
			n++
		}
	}

	// Demand that all fields in struct gets filled
	if n != len(names) {
		diff := stringSliceDiff(names, columns)
		return nil, errors.Errorf("Failed to map all struct fields to query columns (names: %v, columns: %v, diff: %v)", names, columns, diff)
	}

	// Demand that all query columns gets scanned
	if len(columns) > len(ptrs) {
		diff := stringSliceDiff(names, columns)
		return nil, errors.Errorf("Failed to map all query columns to struct fields (names: %v, columns: %v, diff: %v)", names, columns, diff)
	}
	return ptrs, nil
}

func stringSliceDiff(a, b []string) map[string]int {
	diff := map[string]int{}
	for _, name := range a {
		diff[name] = diff[name] + 1
	}
	for _, name := range b {
		diff[name] = diff[name] - 1
	}
	for name, count := range diff {
		if count == 0 {
			delete(diff, name)
		}
	}
	return diff
}

// Map field/column name to canonical name for matching
func canonicalName(name string) string {
	return strings.ToLower(name)
}
