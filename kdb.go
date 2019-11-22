/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/21 13:59
 */
package kdb

import (
	"context"
	"database/sql"
)

func RegisterDataBase(kConf KConfig) {
	for _, dbConf := range kConf.DBConfigList {
		db, err := sql.Open(dbConf.Driver, dbConf.Dsn)
		if err != nil {
			panic(err)
		}
		if dbConf.MaxLifetime > 0 {
			db.SetConnMaxLifetime(dbConf.MaxLifetime)
		}

		if dbConf.MaxIdleConns > 0 {
			db.SetMaxIdleConns(dbConf.MaxIdleConns)
		}

		if dbConf.MaxOpenConns > 0 {
			db.SetMaxOpenConns(dbConf.MaxOpenConns)
		}

		if dbConf.Name == "" {
			dbConf.Name = defaultGroupName
		}
		m.addDB(dbConf.Name, dbConf.IsMaster, db)
	}
}

func Select(query string, bindings ...interface{}) *Rows {
	return newConnection().Select(query, bindings)
}

func Insert(query string, bindings ...interface{}) (LastInsertId int64, err error) {
	return newConnection().Insert(query, bindings)
}

func MultiInsert(query string, bindingsArr [][]interface{}) (LastInsertId []int64, err error) {
	return newConnection().MultiInsert(query, bindingsArr)
}

func Update(query string, bindings ...interface{}) (RowsAffected int64, err error) {
	return newConnection().Update(query, bindings)
}

func Delete(query string, bindings ...interface{}) (RowsAffected int64, err error) {
	return newConnection().Delete(query, bindings)
}

func WithDB(name string) *Connection {
	return newConnection().WithDB(name)
}

func WithContext(ctx context.Context) *Connection {
	return newConnection().WithContext(ctx)
}

func BeginTransaction() (conn *Connection, err error) {

	conn = newConnection()

	err = conn.BeginTransaction()

	if err != nil {
		return nil, err
	}

	return conn, nil
}
