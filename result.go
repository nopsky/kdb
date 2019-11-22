/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/22 14:17
 */
package kdb

import "database/sql"

type Row struct {
	rs *Rows
}

func (r *Row) ToArray() {

}

func (r *Row) ToMap() {

}

func (r *Row) ToStruct(st interface{}) {

}

type Rows struct {
	rs        *sql.Rows
	lastError error
}

func (rs *Rows) ToArray() {

}

func (rs *Rows) ToMap() {

}

func (rs *Rows) ToStruct(sts interface{}) {

}
