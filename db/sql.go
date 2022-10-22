package db

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type JSON map[string]interface{}

func (x *JSON) Value() (driver.Value, error) {
	return json.Marshal(x)
}

func (x *JSON) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("JSON scan source was not []byte")
	}

	return json.Unmarshal(b, &x)
}

func NewInt(v int) *int {
	return &v
}

func NewString(v string) *string {
	return &v
}

func NewBool(v bool) *bool {
	return &v
}

func NewTime(v time.Time) *time.Time {
	return &v
}

type SelectStatement struct {
	selection []string
	from      string
	where     *Clause
	limit     *int
	offset    *int
	order     []string
}

func (s *SelectStatement) Select(selection ...string) *SelectStatement {
	s.selection = selection
	return s
}

func (s *SelectStatement) From(from string) *SelectStatement {
	s.from = from
	return s
}

func (s *SelectStatement) Where(expr *Clause) *SelectStatement {
	s.where = expr
	return s
}

func (s *SelectStatement) Limit(limit int) *SelectStatement {
	s.limit = &limit
	return s
}

func (s *SelectStatement) Offset(offset int) *SelectStatement {
	s.offset = &offset
	return s
}

func (s *SelectStatement) Order(order string) *SelectStatement {
	s.order = append(s.order, order)
	return s
}

func (s *SelectStatement) ToSQL() (string, []interface{}) {
	sql, args := "", []interface{}{}

	sql = fmt.Sprintf("SELECT %s FROM %s", strings.Join(s.selection, ", "), s.from)

	if s.where != nil {
		sql = fmt.Sprintf("%s WHERE %s", sql, s.where.fragment)
		args = append(args, s.where.args...)
	}

	if s.order != nil {
		sql = fmt.Sprintf("%s ORDER BY %s", sql, strings.Join(s.order, ","))
	}

	if s.limit != nil {
		sql = fmt.Sprintf("%s LIMIT %d", sql, *s.limit)
	}

	if s.offset != nil {
		sql = fmt.Sprintf("%s OFFSET %d", sql, *s.offset)
	}

	return Rebind(DOLLAR, sql), args
}

// https://github.com/jmoiron/sqlx/blob/master/bind.go
const (
	UNKNOWN = iota
	QUESTION
	DOLLAR
	NAMED
	AT
)

func Rebind(bindType int, query string) string {
	switch bindType {
	case QUESTION, UNKNOWN:
		return query
	}

	// Add space enough for 10 params before we have to allocate
	rqb := make([]byte, 0, len(query)+10)

	var i, j int

	for i = strings.Index(query, "?"); i != -1; i = strings.Index(query, "?") {
		rqb = append(rqb, query[:i]...)

		switch bindType {
		case DOLLAR:
			rqb = append(rqb, '$')
		case NAMED:
			rqb = append(rqb, ':', 'a', 'r', 'g')
		case AT:
			rqb = append(rqb, '@', 'p')
		}

		j++
		rqb = strconv.AppendInt(rqb, int64(j), 10)

		query = query[i+1:]
	}

	return string(append(rqb, query...))
}

type InsertStatement struct {
	into       string
	columns    []string
	values     []interface{}
	returning  string
	onConflict string
}

func (s *InsertStatement) Into(into string) *InsertStatement {
	s.into = into
	return s
}

func (s *InsertStatement) Columns(columns ...string) *InsertStatement {
	s.columns = columns
	return s
}

func (s *InsertStatement) Values(values ...interface{}) *InsertStatement {
	s.values = values
	return s
}

func (s *InsertStatement) Returning(returning string) *InsertStatement {
	s.returning = returning
	return s
}

func (s *InsertStatement) ToSQL() (string, []interface{}) {
	values := []string{}
	for range s.values {
		values = append(values, "?")
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING %s", s.into, strings.Join(s.columns, ","), strings.Join(values, ","), s.returning)
	return Rebind(DOLLAR, sql), s.values
}

type Table struct {
	Name string
}

type Column struct {
	Name  string
	Table *Table
}

type IntColumn struct {
	Name  string
	Table *Table
}

type StringColumn struct {
	Name  string
	Table *Table
}

type BoolColumn struct {
	Name  string
	Table *Table
}

type Float64Column struct {
	Name  string
	Table *Table
}

type TimeColumn struct {
	Name  string
	Table *Table
}

type JSONColumn struct {
	Name  string
	Table *Table
}

// TODO: clause internal use only
type Clause struct {
	fragment string
	args     []interface{}
}

func NewTable(name string) *Table {
	return &Table{
		Name: name,
	}
}

func (t *Table) NewIntColumn(name string) *IntColumn {
	return &IntColumn{
		Table: t,
		Name:  name,
	}
}

func (t *Table) NewStringColumn(name string) *StringColumn {
	return &StringColumn{
		Table: t,
		Name:  name,
	}
}

func (t *Table) NewTimeColumn(name string) *TimeColumn {
	return &TimeColumn{
		Table: t,
		Name:  name,
	}
}

func (t *Table) NewFloat64Column(name string) *Float64Column {
	return &Float64Column{
		Table: t,
		Name:  name,
	}
}

func (t *Table) NewBoolColumn(name string) *BoolColumn {
	return &BoolColumn{
		Table: t,
		Name:  name,
	}
}

func (t *Table) NewJSONColumn(name string) *JSONColumn {
	return &JSONColumn{
		Table: t,
		Name:  name,
	}
}

func (t *Table) Select(selects ...string) {
}

func (c *IntColumn) EQ(v int) *Clause {
	return &Clause{
		fragment: fmt.Sprintf("%s.%s = ?", c.Table.Name, c.Name),
		args:     []interface{}{v},
	}
}

func (c *StringColumn) EQ(v string) *Clause {
	return &Clause{
		fragment: fmt.Sprintf("%s.%s = ?", c.Table.Name, c.Name),
		args:     []interface{}{v},
	}
}

func And(clauses ...*Clause) *Clause {
	fragments, args := []string{}, []interface{}{}

	for _, clause := range clauses {
		fragments = append(fragments, fmt.Sprintf("(%s)", clause.fragment))
		args = append(args, clause.args...)
	}

	return &Clause{
		fragment: strings.Join(fragments, " AND "),
		args:     args,
	}
}

func Or(clauses ...*Clause) *Clause {
	return &Clause{
		fragment: "or",
		args:     []interface{}{},
	}
}

type UpdateStatement struct {
	table     string
	columns   []string
	values    []interface{}
	where     *Clause
	returning string
}

func (s *UpdateStatement) Table(table string) *UpdateStatement {
	s.table = table
	return s
}

func (s *UpdateStatement) Columns(columns ...string) *UpdateStatement {
	s.columns = columns
	return s
}

func (s *UpdateStatement) Values(values ...interface{}) *UpdateStatement {
	s.values = values
	return s
}

func (s *UpdateStatement) Where(expr *Clause) *UpdateStatement {
	s.where = expr
	return s
}

func (s *UpdateStatement) Returning(returning string) *UpdateStatement {
	s.returning = returning
	return s
}

func (s *UpdateStatement) ToSQL() (string, []interface{}) {
	sql, args := fmt.Sprintf("UPDATE %s SET", s.table), s.values

	sets := []string{}
	for _, col := range s.columns {
		sets = append(sets, fmt.Sprintf("%s = ?", col))
	}

	sql = fmt.Sprintf("%s %s", sql, strings.Join(sets, ","))

	if s.where != nil {
		sql = fmt.Sprintf("%s WHERE %s", sql, s.where.fragment)
		args = append(args, s.where.args...)
	}

	if s.returning != "" {
		sql = fmt.Sprintf("%s RETURNING %s", sql, s.returning)
	}

	return Rebind(DOLLAR, sql), args
}

type DeleteStatemnet struct {
	from  string
	where *Clause
}

func (s *DeleteStatemnet) From(from string) *DeleteStatemnet {
	s.from = from
	return s
}

func (s *DeleteStatemnet) Where(expr *Clause) *DeleteStatemnet {
	s.where = expr
	return s
}

func (s *DeleteStatemnet) ToSQL() (string, []interface{}) {
	sql, args := "", []interface{}{}
	sql = fmt.Sprintf("DELETE FROM %s", s.from)

	if s.where != nil {
		sql = fmt.Sprintf("%s WHERE %s", sql, s.where.fragment)
		args = append(args, s.where.args...)
	}

	return Rebind(DOLLAR, sql), args
}
