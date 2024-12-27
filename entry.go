package gonx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Fields is a shortcut for the map of strings
type Fields map[string]any
type Types map[string]string

// Entry is a parsed log record. Use Get method to retrieve a value by name instead of
// threating this as a map, because inner representation is in design.
type Entry struct {
	Fields Fields
}

// NewEmptyEntry creates an empty Entry to be filled later
func NewEmptyEntry() *Entry {
	return &Entry{Fields: make(Fields)}
}

// NewEntry creates an Entry with fiven fields
func NewEntry(fields Fields) *Entry {
	return &Entry{Fields: fields}
}

// Field returns an entry field value by name or empty string and error if it
// does not exist.
func (entry *Entry) Field(name string) (value any, err error) {
	value, ok := entry.Fields[name]
	if !ok {
		err = fmt.Errorf("field '%v' does not found in record %+v", name, *entry)
	}
	return
}

func (entry *Entry) StringField(name string) (value string, err error) {
	tmp, err := entry.Field(name)
	if err != nil {
		return
	}
	switch tmp := tmp.(type) {
	case float64:
		value = strconv.FormatFloat(tmp, 'f', 2, 64)
	case int:
		value = strconv.Itoa(tmp)
	case int64:
		value = strconv.FormatInt(tmp, 10)
	case uint64:
		value = strconv.FormatUint(tmp, 10)
	case string:
		value = tmp
	}
	return
}

// FloatField returns an entry field value as float64. Return nil if field does not exist
// and conversion error if cannot cast a type.
func (entry *Entry) FloatField(name string) (value float64, err error) {
	tmp, err := entry.Field(name)
	if err != nil {
		return
	}
	switch tmp := tmp.(type) {
	case float64:
		value = tmp
	case int:
		value = float64(tmp)
	case int64:
		value = float64(tmp)
	case uint64:
		value = float64(tmp)
	case string:
		value, err = strconv.ParseFloat(tmp, 64)
	default:
	}
	return
}

// IntField returns an entry field value as float64. Return nil if field does not exist
// and conversion error if cannot cast a type.
func (entry *Entry) Int64Field(name string) (value int64, err error) {
	tmp, err := entry.Field(name)
	if err != nil {
		return
	}
	switch tmp := tmp.(type) {
	case float64:
		value = int64(tmp)
	case int:
		value = int64(tmp)
	case int64:
		value = tmp
	case uint64:
		value = int64(tmp)
	case string:
		value, err = strconv.ParseInt(tmp, 0, 64)
	}
	return
}

// IntField returns an entry field value as IntField but only int not int64. Return nil if field does not exist
// and conversion error if cannot cast a type.
func (entry *Entry) IntField(name string) (value int, err error) {
	tmp, err := entry.Field(name)
	if err != nil {
		return
	}
	switch tmp := tmp.(type) {
	case float64:
		value = int(tmp)
	case int:
		value = tmp
	case int64:
		value = int(tmp)
	case uint64:
		value = int(tmp)
	case string:
		value, err = strconv.Atoi(tmp)
	}
	return
}

// EntryField returns an entry field value as Entry.
func (entry *Entry) EntryField(name string) (value *Entry, err error) {
	tmp, err := entry.Field(name)
	if err != nil {
		return
	}
	switch tmp := tmp.(type) {
	case *Entry:
		value = tmp
	}
	return
}

func (entry *Entry) EntryList(name string) (values []*Entry, err error) {
	tmp, err := entry.Field(name)
	if err != nil {
		return
	}
	switch tmp := tmp.(type) {
	case []*Entry:
		values = tmp
	}
	return
}

// SetField sets the value of a field
func (entry *Entry) SetField(name string, value any) {
	entry.Fields[name] = value
}

// SetFloatField is a Float field value setter. It accepts float64, but still store it as a
// string in the same fields map. The precision is 2, its enough for log
// parsing task
func (entry *Entry) SetFloatField(name string, value float64) {
	entry.SetField(name, value)
}

// SetUintField is a Integer field value setter. It accepts float64, but still store it as a
// string in the same fields map.
func (entry *Entry) SetUintField(name string, value uint64) {
	entry.SetField(name, value)
}

// SetField sets the value of a Entry
func (entry *Entry) SetEntryField(name string, value *Entry) {
	entry.SetField(name, value)
}

// SetField sets the value of a Entry
func (entry *Entry) SetEntryList(name string, values []*Entry) {
	entry.SetField(name, values)
}

// Merge two entries by updating values for master entry with given.
func (entry *Entry) Merge(merge *Entry) {
	for name, value := range merge.Fields {
		entry.SetField(name, value)
	}
}

// FieldsHash returns a hash of all fields
func (entry *Entry) FieldsHash(fields []string) string {
	var key []string
	for _, name := range fields {
		value, err := entry.Field(name)
		if err != nil {
			value = "NULL"
		}
		key = append(key, fmt.Sprintf("'%v'=%v", name, value))
	}
	return strings.Join(key, ";")
}

// Partial returns a partial field entry with the specified fields
func (entry *Entry) Partial(fields []string) *Entry {
	partial := NewEmptyEntry()
	for _, name := range fields {
		value, _ := entry.Field(name)
		partial.SetField(name, value)
	}
	return partial
}

func (entry Entry) MarshalJSON() ([]byte, error) {

	if data, err := json.Marshal(entry.Fields); err == nil {
		return data, nil
	} else {
		return nil, err
	}
}

func (entry *Entry) UnmarshalJSON(data []byte) error {

	e := Entry{}
	if err := json.Unmarshal(data, &e.Fields); err != nil {
		return err
	}

	*entry = e

	return nil
}
