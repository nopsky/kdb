/**
 * @Author : nopsky
 * @email : zhanglijun@jiayunhui.com
 * @Date :  16:22
 */
package kdb

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Builder struct {
	table      string
	conn       *Connection
	grammar    *Grammar
	distinct   bool
	bindings   map[string][]interface{}
	columns    []string
	agg        *aggregate
	joins      []*join
	wheres     []*where
	groups     []string
	havings    []*where
	orders     []*order
	unions     []*union
	offsetFlag bool
	offset     int
	limitFlag  bool
	limit      int
}

type aggregate struct {
	column   string
	function string
}

type join struct {
	table    string
	column   string
	operator string
	value    string
	glue     string
	typ      string
}

type where struct {
	typ      string
	column   interface{}
	operator string
	value    interface{}
	values   []interface{}
	glue     string
}

type order struct {
	column    string
	direction string
}

type union struct {
	query *Builder
	all   bool
}

func newBuilder(conn *Connection, grammar *Grammar) *Builder {
	b := new(Builder)
	b.conn = conn
	b.grammar = grammar
	b.columns = make([]string, 0)
	b.bindings = make(map[string][]interface{})
	b.wheres = make([]*where, 0)
	return b
}

func (b *Builder) Table(table string) *Builder {
	b.table = table
	return b
}

func (b *Builder) Select(columns ...string) *Builder {

	if len(columns) == 0 {
		columns = append(columns, "*")
	}

	b.columns = columns
	return b
}

func (b *Builder) Distinct() *Builder {
	b.distinct = true
	return b
}

func (b *Builder) join(table string, column string, operator string, value string, typ string, glue string) *Builder {
	j := new(join)
	j.table = table
	j.column = column
	j.operator = operator
	j.value = value
	j.typ = typ
	j.glue = glue
	if b.joins == nil {
		b.joins = []*join{j}
	} else {
		b.joins = append(b.joins, j)
	}
	return b
}

func (b *Builder) LeftJoin(table, first, operator, second string) *Builder {
	return b.join(table, first, operator, second, "left", "and")
}

func (b *Builder) RightJoin(table, first, operator, second string) *Builder {
	return b.join(table, first, operator, second, "right", "and")
}

func (b *Builder) InnerJoin(table, first, operator, second string) *Builder {
	return b.join(table, first, operator, second, "inner", "and")
}

func (b *Builder) Where(column interface{}, args ...interface{}) *Builder {

	if len(args) == 0 {
		return b.WhereIsNull(column)
	}

	w := new(where)
	w.column = column
	w.glue = "and"
	w.typ = "basic"

	switch len(args) {
	case 1:
		w.operator = "="
		w.value = args[0]
	case 2:
		w.operator = args[0].(string)
		w.value = args[1]
	case 3:
		w.operator = args[0].(string)
		w.value = args[1]
		w.glue = args[2].(string)
	case 4:
		w.operator = args[0].(string)
		w.value = args[1]
		w.glue = args[2].(string)
		w.typ = args[3].(string)
	}

	b.addBinding("where", []interface{}{w.value})

	b.wheres = append(b.wheres, w)

	return b
}

func (b *Builder) WhereIsNull(column interface{}) *Builder {
	w := new(where)
	w.column = column
	w.glue = "and"
	w.typ = "null"
	w.operator = "is"
	w.value = "null"
	b.wheres = append(b.wheres, w)
	return b
}

func (b *Builder) OrWhere(column interface{}, args ...interface{}) *Builder {
	if len(args) == 0 {
		return b.orWhereIsNull(column)
	}

	w := new(where)
	w.column = column
	w.glue = "or"
	w.typ = "basic"

	switch len(args) {
	case 1:
		w.operator = "="
		w.value = args[0]
	case 2:
		w.operator = args[0].(string)
		w.value = args[1]
	case 3:
		w.operator = args[0].(string)
		w.value = args[1]
		w.glue = args[2].(string)
	case 4:
		w.operator = args[0].(string)
		w.value = args[1]
		w.glue = args[2].(string)
		w.typ = args[3].(string)
	}

	b.addBinding("where", []interface{}{w.value})

	b.wheres = append(b.wheres, w)

	return b
}

func (b *Builder) orWhereIsNull(column interface{}) *Builder {
	w := new(where)
	w.column = column
	w.glue = "or"
	w.typ = "null"
	w.operator = "is"
	w.value = "null"
	b.wheres = append(b.wheres, w)
	return b
}

func (b *Builder) WhereIn(column interface{}, values interface{}) *Builder {
	w := new(where)
	w.column = column
	w.glue = "and"
	w.typ = "in"
	w.operator = "in"
	b.wheres = append(b.wheres, w)

	v := reflect.ValueOf(values)
	if v.Kind() == reflect.Slice {
		w.values = make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			w.values[i] = v.Index(i).Interface()
		}
	}
	b.addBinding("where", w.values)
	return b
}

func (b *Builder) WhereNotIn(column interface{}, values interface{}) *Builder {
	w := new(where)
	w.column = column
	w.glue = "and"
	w.typ = "in"
	w.operator = "not in"
	b.wheres = append(b.wheres, w)

	v := reflect.ValueOf(values)
	if v.Kind() == reflect.Slice {
		w.values = make([]interface{}, v.Len())
		for i := 0; i < v.Len(); i++ {
			w.values[i] = v.Index(i).Interface()
		}
	}
	b.addBinding("where", w.values)
	return b
}

func (b *Builder) GroupBy(columns ...string) *Builder {
	if len(columns) > 0 {
		b.groups = columns
	}
	return b
}

func (b *Builder) Having(column interface{}, args ...interface{}) *Builder {
	w := new(where)
	w.column = column
	w.glue = "and"
	w.typ = "basic"

	switch len(args) {
	case 0:
		w.operator = "="
		w.value = nil
	case 1:
		w.operator = "="
		w.value = args[0]
	case 2:
		w.operator = args[0].(string)
		w.value = args[1]
	case 3:
		w.operator = args[0].(string)
		w.value = args[1]
		w.glue = args[2].(string)
	case 4:
		w.operator = args[0].(string)
		w.value = args[1]
		w.glue = args[2].(string)
		w.typ = args[3].(string)
	}

	b.addBinding("having", []interface{}{w.value})

	if b.havings == nil {
		b.havings = []*where{w}
	} else {
		b.havings = append(b.havings, w)
	}

	return b
}

func (b *Builder) OrderBy(column string, direction ...string) *Builder {
	var direct string
	if len(direction) == 0 {
		direct = "asc"
	} else {
		direct = direction[0]
	}

	o := new(order)
	o.column = column
	o.direction = direct

	if b.orders == nil {
		b.orders = []*order{o}
	}

	return b
}

func (b *Builder) Offset(offset int) *Builder {
	b.offset = offset
	b.offsetFlag = true
	return b
}

func (b *Builder) Limit(limit int) *Builder {
	if limit > 0 {
		b.limitFlag = true
		b.limit = limit
	}
	return b
}

func (b *Builder) Union(query *Builder, all ...bool) *Builder {
	var allFlag bool
	if len(all) > 0 {
		allFlag = all[0]
	}

	u := new(union)
	u.query = query
	u.all = allFlag
	if b.unions == nil {
		b.unions = []*union{u}
	} else {
		b.unions = append(b.unions, u)
	}

	b.addBinding("union", query.getBindings())

	return b
}

func (b *Builder) Count(columns ...string) (int64, error) {
	var column string

	if len(columns) == 0 {
		column = "*"
	} else {
		column = columns[0]
	}

	result, err := b.aggregate("count", column)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	return strconv.ParseInt(result, 10, 64)
}

func (b *Builder) Min(column string) (interface{}, error) {
	return b.aggregate("min", column)
}

func (b *Builder) Max(column string) (interface{}, error) {
	return b.aggregate("max", column)
}

func (b *Builder) Sum(column string) (interface{}, error) {
	return b.aggregate("sum", column)

}

func (b *Builder) Avg(column string) (interface{}, error) {
	return b.aggregate("avg", column)
}

func (b *Builder) aggregate(function string, column string) (string, error) {
	b.columns = b.columns[:0]
	delete(b.bindings, "select")

	b.setAggregate(function, column)
	result, err := b.Get(column).ToMap()
	if err != nil {
		return "", err
	}

	if len(result) == 0 {
		return "", sql.ErrNoRows
	}

	return result[0]["aggregate"], nil
}

func (b *Builder) setAggregate(function string, column string) *Builder {

	b.agg = new(aggregate)
	b.agg.function = function
	b.agg.column = column

	if len(b.groups) == 0 {
		b.orders = nil
		delete(b.bindings, "order")
	}

	return b
}

func (b *Builder) First(columns ...string) *Row {
	rs := b.Get(columns...)
	r := new(Row)
	r.rs = rs
	return r
}

func (b *Builder) Get(columns ...string) *Rows {
	if len(columns) > 0 {
		b.Select(columns...)
	}
	return b.runSelect()
}

func (b *Builder) Insert(data interface{}) (lastInsertId int64, err error) {

	columns, values, err := b.getInsertMap(data)
	if err != nil {
		return 0, err
	}

	b.columns = append(b.columns, columns...)

	bindings := make([]interface{}, len(columns))

	for i, column := range columns {
		bindings[i] = values[column][0]
	}

	b.addBinding("insert", bindings)

	if len(b.columns) > 0 {
		sql := b.grammar.compileInsert(b)
		return b.conn.Insert(sql, b.getBindings())
	}

	return 0, errors.New("insert data cannot be empty")
}

func (b *Builder) MultiInsert(data interface{}) (lastInsertId []int64, err error) {

	stVal := reflect.ValueOf(data)
	if stVal.Kind() != reflect.Slice {
		return nil, errors.New("data is not []interface{} type")
	}

	n := stVal.Len()

	if n > 0 {
		columns, values, err := b.getInsertMap(data)
		if err != nil {
			return nil, err
		}

		b.columns = append(b.columns, columns...)

		bindingsArr := make([][]interface{}, n)

		for i := 0; i < n; i++ {
			bindings := make([]interface{}, len(columns))
			for j, column := range columns {
				bindings[j] = values[column][i]
			}
			bindingsArr[i] = bindings
		}

		if len(b.columns) > 0 {
			sql := b.grammar.compileInsert(b)
			return b.conn.MultiInsert(sql, bindingsArr)
		}
	}

	return nil, errors.New("insert data cannot be empty")
}

func (b *Builder) getInsertMap(data interface{}) (columns []string, values map[string][]interface{}, err error) {
	stValue := reflect.Indirect(reflect.ValueOf(data))

	values = make(map[string][]interface{}, 0)
	switch stValue.Kind() {
	case reflect.Struct:
		var ignore bool
		for i := 0; i < stValue.NumField(); i++ {

			v := reflect.Indirect(stValue.Field(i))

			//处理嵌套的struct中的db映射字段
			if v.Kind() == reflect.Struct {

				var ignore bool

				switch v.Interface().(type) {
				case time.Time:
					ignore = true
				case sql.NullTime:
					ignore = true
				case sql.NullString:
					ignore = true
				case sql.NullBool:
					ignore = true
				case sql.NullInt64:
					ignore = true
				case sql.NullInt32:
					ignore = true
				case sql.NullFloat64:
					ignore = true
				}

				if !ignore {
					cols, vals, err := b.getInsertMap(v.Interface())
					if err != nil {
						return nil, nil, err
					}

					for _, column := range cols {
						if _, ok := values[column]; !ok {
							columns = append(columns, column)
						}
					}

					for column, v := range vals {
						if _, ok := values[column]; ok {
							values[column] = append(values[column], v...)
						} else {
							values[column] = v
						}
					}
				}
			}

			tag := stValue.Type().Field(i).Tag.Get(kdb.structTag)
			attrList := strings.Split(tag, ";")
			ignore = false

			if len(attrList) > 1 {
				for _, attr := range attrList {
					if attr == "auto" {
						ignore = true
						break
					}
				}
			}

			if ignore {
				continue
			}

			column := attrList[0]
			if column != "" {
				if _, ok := values[column]; ok {
					values[column] = append(values[column], v.Interface())
				} else {
					columns = append(columns, column)
					values[column] = []interface{}{v.Interface()}
				}
			}
		}
	case reflect.Map:
		keys := stValue.MapKeys()
		for _, k := range keys {
			column := k.String()
			if _, ok := values[column]; ok {
				values[column] = append(values[column], stValue.MapIndex(k).Interface())
			} else {
				columns = append(columns, column)
				values[column] = []interface{}{stValue.MapIndex(k).Interface()}
			}
		}
	case reflect.Slice:
		n := stValue.Len()
		for i := 0; i < n; i++ {

			item := stValue.Index(i)
			cols, vals, err := b.getInsertMap(item.Interface())

			if err != nil {
				return nil, nil, err
			}

			for _, column := range cols {
				if _, ok := values[column]; !ok {
					columns = append(columns, column)
				}
			}

			for column, v := range vals {
				if _, ok := values[column]; ok {
					values[column] = append(values[column], v...)
				} else {
					values[column] = v
				}
			}
		}
	}
	return
}

func (b *Builder) Update(data map[string]interface{}) (affectRows int64, err error) {

	if len(data) > 0 {
		bindings := make([]interface{}, len(data))
		i := 0
		for k, v := range data {
			b.columns = append(b.columns, k)
			bindings[i] = v
			i++
		}
		b.addBinding("update", bindings)
		sql := b.grammar.compileUpdate(b)
		return b.conn.Update(sql, b.getBindings())
	}

	return 0, errors.New("update data cannot be empty")

}

func (b *Builder) Delete() (affectRows int64, err error) {
	sql := b.grammar.compileDelete(b)
	return b.conn.Delete(sql, b.getBindings())
}

func (b *Builder) addBinding(typ string, value []interface{}) {
	if _, ok := b.bindings[typ]; ok {
		b.bindings[typ] = append(b.bindings[typ], value...)
	} else {
		b.bindings[typ] = value
	}
}

func (b *Builder) toSQL() string {
	return b.grammar.compileSelect(b)
}

func (b *Builder) runSelect() *Rows {
	return b.conn.Select(b.toSQL(), b.getBindings())
}

func (b *Builder) getBindings() (bindings []interface{}) {

	bindings = make([]interface{}, 0)

	if v, ok := b.bindings["join"]; ok {
		bindings = append(bindings, v...)
	}

	if v, ok := b.bindings["insert"]; ok {
		bindings = append(bindings, v...)
	}

	if v, ok := b.bindings["update"]; ok {
		bindings = append(bindings, v...)
	}

	if v, ok := b.bindings["where"]; ok {
		bindings = append(bindings, v...)
	}

	if v, ok := b.bindings["having"]; ok {
		bindings = append(bindings, v...)
	}

	if v, ok := b.bindings["union"]; ok {
		bindings = append(bindings, v...)
	}

	return
}
