/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/22 14:20
 */
package kdb

import (
	"context"
	"database/sql"
	"errors"
	"log"
)

type Connection struct {
	ctx  context.Context
	conn *sql.Conn
	tx   *sql.Tx
	name string
}

func newConnection() *Connection {
	c := new(Connection)
	c.ctx = context.Background()
	return c
}

func (c *Connection) WithDB(name string) *Connection {
	c.name = name
	return c
}

func (c *Connection) WithContext(ctx context.Context) *Connection {
	c.ctx = ctx
	return c
}

func (c *Connection) Select(query string, bindings []interface{}) *Rows {

	rows, err := c.queryRows(query, bindings)

	if err != nil {
		return &Rows{rs: nil, lastError: err}
	}

	return &Rows{rs: rows, lastError: err}
}

func (c *Connection) Insert(query string, bindings []interface{}) (int64, error) {

	rs, err := c.exec(query, bindings)

	if err != nil {
		return 0, err
	}

	return rs.LastInsertId()
}

func (c *Connection) MultiInsert(query string, bindingsArr [][]interface{}) ([]int64, error) {
	var stmt *sql.Stmt
	var err error

	if c.tx != nil {
		stmt, err = c.tx.PrepareContext(c.ctx, query)
	} else {
		var conn *sql.Conn
		conn, err = c.getConn()

		if err != nil {
			return nil, err
		}
		stmt, err = conn.PrepareContext(c.ctx, query)
	}

	if err != nil {
		return nil, err
	}

	defer stmt.Close()

	lastInsertIds := make([]int64, 0)

	for _, bindings := range bindingsArr {
		rs, err := stmt.ExecContext(c.ctx, bindings...)
		if err != nil {
			return nil, err
		}

		lastInsertId, err := rs.LastInsertId()

		if err != nil {
			return nil, err
		}

		lastInsertIds = append(lastInsertIds, lastInsertId)
	}

	return lastInsertIds, nil
}

func (c *Connection) Update(query string, bindings []interface{}) (int64, error) {
	rs, err := c.exec(query, bindings)

	if err != nil {
		return 0, err
	}

	return rs.RowsAffected()
}

func (c *Connection) Delete(query string, bindings []interface{}) (int64, error) {

	rs, err := c.exec(query, bindings)

	if err != nil {
		return 0, err
	}

	return rs.RowsAffected()
}

func (c *Connection) BeginTransaction() error {
	if c.tx == nil {
		conn, err := c.getConn()

		if err != nil {
			return err
		}

		tx, err := conn.BeginTx(c.ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})

		if err != nil {
			return err
		}

		c.tx = tx
	}

	return nil
}

func (c *Connection) Commit() error {
	if c.tx == nil {
		return errors.New("no beginTx")
	}
	return c.tx.Commit()
}

func (c *Connection) RollBack() error {
	if c.tx == nil {
		return errors.New("no beginTx")
	}

	return c.tx.Rollback()
}

func (c *Connection) queryRows(query string, bindings []interface{}) (rows *sql.Rows, err error) {

	log.Println("query:", query, "| bindings:", bindings)

	if c.tx != nil {
		rows, err = c.tx.QueryContext(c.ctx, query, bindings...)
		return
	}

	var conn *sql.Conn

	conn, err = c.getConn()

	if err != nil {
		return nil, err
	}

	rows, err = conn.QueryContext(c.ctx, query, bindings...)

	return
}

func (c *Connection) exec(query string, bindings []interface{}) (rs sql.Result, err error) {

	log.Println("exec:", query, "| bindings:", bindings)

	if c.tx != nil {
		rs, err = c.tx.ExecContext(c.ctx, query, bindings...)

		return
	}

	var conn *sql.Conn

	conn, err = c.getConn()

	if err != nil {
		return nil, err
	}

	rs, err = conn.ExecContext(c.ctx, query, bindings...)

	return
}

func (c *Connection) getConn() (*sql.Conn, error) {

	var err error

	var db *sql.DB

	if c.conn != nil {
		return c.conn, nil
	}

	if c.name != "" {
		db, err = m.getDB(c.name)
	} else {
		db, err = m.getDB()
	}

	if err != nil {
		return nil, err
	}

	conn, err := db.Conn(c.ctx)

	if err != nil {

		return nil, err
	}

	c.conn = conn

	return c.conn, nil
}

func (c *Connection) Table(table string) *Builder {
	return c.query().Table(table)
}

func (c *Connection) query() *Builder {
	g := NewGrammar()
	b := newBuilder(c, g)
	return b
}
