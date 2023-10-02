package sqltest

import (
	"reflect"
)

func IsPtr(val interface{}) bool {
	return reflect.TypeOf(val).Kind() == reflect.Ptr
}

// Unwrap pointers to pointers to pointers, second return arg is false if nil pointer found
func UnwrapPointers(val interface{}) (elem interface{}, ok bool) {
	for IsPtr(val) {
		ptr := reflect.ValueOf(val)
		if ptr.IsNil() {
			return val, false
		}
		if ptr.Elem().CanInterface() {
			val = ptr.Elem().Interface()
		}
	}
	return val, true
}

func MustStructValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	// Note not else-if, allowing unwrapping of an interface containing a pointer to a struct
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("Expecting a struct or pointer to struct, possibly wrapped in interface.")
	}
	return v
}

func MustStructType(v reflect.Type) reflect.Type {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	// Note not else-if, allowing unwrapping of an interface containing a pointer to a struct
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		panic("Expecting a struct or pointer to struct, possibly wrapped in interface.")
	}
	return v
}

// Return the name of the struct, possibly behind an
// interface holding a pointer to the struct instance.
func UnderlyingStructName(obj interface{}) string {
	return MustStructType(reflect.TypeOf(obj)).Name()
}

func NamesOf(structType reflect.Type) []string {
	n := structType.NumField()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		field := structType.Field(i)
		names = append(names, field.Name)
	}
	return names
}

// Return names of fields of struct instance v (does not recurse into struct members or embedded structs)
func FieldNames(v interface{}) []string {
	t := MustStructType(reflect.TypeOf(v))
	n := t.NumField()
	names := make([]string, n)
	for i := range names {
		names[i] = t.Field(i).Name
	}
	return names
}

func deepFieldsOfStructValue(val reflect.Value) []reflect.Value {
	v := MustStructValue(val)
	n := v.NumField()
	fields := make([]reflect.Value, 0, n)
	for i := 0; i < n; i++ {
		f := v.Field(i)
		tf := v.Type().Field(i)
		k := tf.Type.Kind()
		if k == reflect.Struct && (tf.Anonymous || tf.Tag.Get("refl") == "recurse") {
			fields = append(fields, deepFieldsOfStructValue(f)...)
		} else {
			fields = append(fields, f)
		}
	}
	return fields
}

// Return names of fields of struct instance v, recursing into embedded structs (but not named struct members)
func DeepFieldNames(v interface{}) []string {
	return deepFieldNamesOfStructType(reflect.TypeOf(v))
}

func deepFieldNamesOfStructType(typ reflect.Type) []string {
	t := MustStructType(typ)
	n := t.NumField()
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		f := t.Field(i)
		k := f.Type.Kind()
		if k == reflect.Struct && (f.Anonymous || f.Tag.Get("refl") == "recurse") {
			names = append(names, deepFieldNamesOfStructType(f.Type)...)
		} else {
			names = append(names, t.Field(i).Name)
		}
	}
	return names
}

// Return pointers to fields of struct instance v, recursing into embedded structs (but not named struct members)
func DeepFieldPointers(obj interface{}) []interface{} {
	fields := deepFieldsOfStructValue(reflect.ValueOf(obj))
	pointers := make([]interface{}, len(fields))
	for i, f := range fields {
		if f.CanInterface() {
			pointers[i] = f.Addr().Interface()
		}
	}
	return pointers
}

// Return pointers to fields of struct instance v (does not recurse into struct members or embedded structs)
func FieldPointers(obj interface{}) []interface{} {
	v := MustStructValue(reflect.ValueOf(obj))
	n := v.NumField()
	pointers := make([]interface{}, n)
	for i := range pointers {
		if v.Field(i).Addr().CanInterface() {
			pointers[i] = v.Field(i).Addr().Interface()
		}
	}
	return pointers
}

// Return values of fields of struct instance v, recursing into embedded structs (but not named struct members)
func DeepFieldValues(obj interface{}) []interface{} {
	fields := deepFieldsOfStructValue(reflect.ValueOf(obj))
	values := make([]interface{}, len(fields))
	for i, f := range fields {
		if f.CanInterface() {
			values[i] = f.Interface()
		}
	}
	return values
}

// Return field values of struct instance v (does not recurse into struct members or embedded structs)
func FieldValues(obj interface{}) []interface{} {
	v := MustStructValue(reflect.ValueOf(obj))
	n := v.NumField()
	values := make([]interface{}, n)
	for i := range values {
		if v.Field(i).CanInterface() {
			values[i] = v.Field(i).Interface()
		}
	}
	return values
}

type NameAndTag struct {
	Name string
	Tag  string
}

func FieldNamesAndTags(v interface{}, tagKey string) []NameAndTag {
	t := MustStructType(reflect.TypeOf(v))
	n := t.NumField()
	names := make([]NameAndTag, n)
	for i := range names {
		f := t.Field(i)
		names[i] = NameAndTag{
			Name: f.Name,
			Tag:  f.Tag.Get(tagKey),
		}
	}
	return names
}

// Prefix each value in names with prefix string
func Prefixed(prefix string, names []string) []string {
	p := make([]string, len(names))
	for i, name := range names {
		p[i] = prefix + name
	}
	return p
}

// Return a string representation of sql zero literal for given type.
// This is used with coalesce to map nulls after left join to zeros
// in some queries generated via reflection on struct types.
func SqlZeroString(t reflect.Type) string {
	kind := t.Kind()
	if kind == reflect.Interface {
		kind = t.Elem().Kind()
	}

	switch kind {
	case reflect.String:
		return `''`

	case reflect.Float32, reflect.Float64:
		return `0.0`

	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return `0`

	case reflect.Ptr:
		return `null`

	case reflect.Array:
		switch t.Name() {
		case "UniqueIdentifier":
			return `CAST('00000000-0000-0000-0000-000000000000' AS uniqueidentifier)`
		default:
			return `null`
		}

	case reflect.Struct:
		switch t.Name() {
		case "Time":
			return `'0001-01-01'`
		case "Date":
			return `'0001-01-01'`
		case "Money":
			return `0.00`
		case "GUID":
			return `CAST('00000000-0000-0000-0000-000000000000' AS uniqueidentifier)`
		case "Decimal":
			return `0.00`
		}
	}

	panic("Unhandled type")
}
