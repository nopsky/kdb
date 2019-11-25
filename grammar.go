/**
 * @Author : nopsky
 * @email : zhanglijun@jiayunhui.com
 * @Date :  11:20
 */
package kdb

import (
	"bytes"
	"fmt"
	"strings"
)

type Grammar struct {
}

func NewGrammar() *Grammar {
	return new(Grammar)
}

func (g *Grammar) compileSelect(b *Builder) string {

	if len(b.columns) == 0 {
		b.columns = []string{"*"}
	}

	return fmt.Sprintf("select %s", strings.TrimSpace(strings.Join(g.compileComponents(b), " ")))
}

func (g *Grammar) compileUpdate(b *Builder) string {

	var sql string

	comm := ""
	for _, column := range b.columns {
		sql = fmt.Sprintf("%s%s %s = %s", strings.TrimSpace(sql), comm, g.wrapTable(column), "?")
		comm = ","
	}

	sql = fmt.Sprintf("update %s set %s %s", g.wrapTable(b.table), sql, g.compileWheres(b))

	return sql
}

func (g *Grammar) compileInsert(b *Builder) string {

	var sql string
	comm := ""

	for _, column := range b.columns {
		sql = fmt.Sprintf("%s%s %s", strings.TrimSpace(sql), comm, g.wrapColumn(column))
		comm = ","
	}

	placeHolder := strings.Repeat("?,", len(b.columns))

	sql = fmt.Sprintf("insert into %s (%s) values (%s) ", g.wrapTable(b.table), strings.TrimSpace(sql), placeHolder[:len(placeHolder)-1])

	return sql
}

func (g *Grammar) compileDelete(b *Builder) string {
	sql := fmt.Sprintf("delete %s %s", g.compileFrom(b), g.compileWheres(b))
	return strings.TrimSpace(sql)
}

func (g *Grammar) compileComponents(b *Builder) []string {
	sql := make([]string, 0)

	if b.agg != nil {
		sql = append(sql, g.compileAggregate(b))
	}

	if len(b.columns) > 0 && b.agg == nil {
		sql = append(sql, g.compileColumns(b))
	}

	if b.table != "" {
		sql = append(sql, g.compileFrom(b))
	}

	if len(b.joins) > 0 {
		sql = append(sql, g.compileJoins(b))
	}

	if len(b.wheres) > 0 {
		whereSql := g.compileWheres(b)
		if whereSql != "" {
			sql = append(sql, whereSql)
		}
	}

	if len(b.groups) > 0 {
		sql = append(sql, g.compileGroups(b))
	}

	if len(b.havings) > 0 {
		sql = append(sql, g.compileHaving(b))
	}

	if len(b.orders) > 0 {
		sql = append(sql, g.compileOrders(b))
	}

	if b.offsetFlag {
		sql = append(sql, g.compileOffset(b))
	}

	if b.limitFlag {
		sql = append(sql, g.compileLimit(b))
	}

	if len(b.unions) > 0 {
		sql = append(sql, g.compileUnions(b))
	}
	return sql
}

func (g *Grammar) compileAggregate(b *Builder) string {
	column := b.agg.column
	if b.distinct && b.agg.column != "*" {
		column = fmt.Sprintf("distinct %s", g.wrapColumn(column))
	}

	return fmt.Sprintf("%s(%s) as aggregate", b.agg.function, g.wrapColumn(column))
}

func (g *Grammar) compileColumns(b *Builder) string {

	if b.distinct {
		return fmt.Sprintf("distinct %s", g.wrapColumn(b.columns...))
	}

	return g.wrapColumn(b.columns...)

}

func (g *Grammar) compileFrom(b *Builder) string {
	return fmt.Sprintf("from %s", g.wrapTable(b.table))
}

func (g *Grammar) compileJoins(b *Builder) string {
	var sql string
	comm := ""
	for _, v := range b.joins {
		sql = fmt.Sprintf("%s %s %s join %s on %s %s %s", strings.TrimSpace(sql), comm, v.typ, g.wrapTable(v.table), g.wrapColumn(v.column), v.operator, g.wrapColumn(v.value))
		comm = v.glue
	}
	return strings.TrimSpace(sql)
}

func (g *Grammar) compileWheres(b *Builder) string {

	var sql string

	for k, w := range b.wheres {
		if k == 0 {
			w.glue = ""
		}

		switch w.typ {
		case "basic":
			sql = fmt.Sprintf("%s %s %s %s %s", strings.TrimSpace(sql), w.glue, g.wrapColumn(w.column.(string)), w.operator, "?")
		case "null":
			sql = fmt.Sprintf("%s %s %s %s %s", strings.TrimSpace(sql), w.glue, g.wrapColumn(w.column.(string)), w.operator, w.value)
		case "in":
			placeHolder := strings.Repeat("?,", len(w.values))
			sql = fmt.Sprintf("%s %s %s %s (%s)", strings.TrimSpace(sql), w.glue, g.wrapColumn(w.column.(string)), w.operator, placeHolder[:len(placeHolder)-1])
		}
	}

	return fmt.Sprintf("where %s", strings.TrimSpace(sql))
}

func (g *Grammar) compileGroups(b *Builder) string {
	buf := bytes.NewBufferString("group by ")
	comm := ""
	for _, column := range b.groups {
		buf.WriteString(strings.TrimSpace(fmt.Sprintf("%s %s", comm, g.wrapColumn(column))))
		comm = ","
	}

	return buf.String()
}

func (g *Grammar) compileHaving(b *Builder) string {
	var sql string

	for k, v := range b.havings {
		if k == 0 {
			v.glue = ""
		}
		sql = fmt.Sprintf("%s %s %s %s %s", strings.TrimSpace(sql), v.glue, g.wrapColumn(v.column.(string)), v.operator, "?")
	}

	return fmt.Sprintf("having %s", strings.TrimSpace(sql))
}

func (g *Grammar) compileOrders(b *Builder) string {
	buf := bytes.NewBufferString("order by ")
	comm := ""
	for _, o := range b.orders {
		buf.WriteString(strings.TrimSpace(fmt.Sprintf("%s %s %s", comm, g.wrapColumn(o.column), o.direction)))
		comm = ","
	}

	return buf.String()
}

func (g *Grammar) compileOffset(b *Builder) string {
	return fmt.Sprintf("offset %d", b.offset)
}

func (g *Grammar) compileLimit(b *Builder) string {
	return fmt.Sprintf("limit %d", b.limit)
}

func (g *Grammar) compileUnions(b *Builder) string {
	var sql string
	for _, v := range b.unions {
		if v.all {
			sql = fmt.Sprintf("%s union all %s", strings.TrimSpace(sql), v.query.toSQL())
		} else {
			sql = fmt.Sprintf("%s union %s", strings.TrimSpace(sql), v.query.toSQL())
		}
	}
	return sql
}

func (g *Grammar) wrapTable(table string) string {
	return fmt.Sprintf("%s%s", kdb.tablePrefix, table)
}

func (g *Grammar) wrapColumn(columns ...string) string {
	for i, column := range columns {
		segments := strings.Split(column, ".")
		if len(segments) > 1 {
			segments[0] = g.wrapTable(segments[0])
			if segments[1] != "*" && !strings.Contains(segments[0], "->") {
				segments[1] = fmt.Sprintf("`%s`", segments[1])
			}
		} else {
			if segments[0] != "*" && !strings.Contains(segments[0], "->") {
				segments[0] = fmt.Sprintf("`%s`", segments[0])
			}
		}
		column = strings.Join(segments, ".")
		columns[i] = column
	}
	return fmt.Sprintf("%s", strings.Join(columns, ", "))
}
