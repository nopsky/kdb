/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/22 14:17
 */
package kdb

import (
	"database/sql"
	"fmt"
	"reflect"
)

type Rows struct {
	rs        *sql.Rows
	lastError error
}

func (r *Rows) ToArray() (data [][]string, err error) {

	if r.rs == nil {
		return nil, r.lastError
	}

	defer r.rs.Close()

	//获取查询的字段
	fields, err := r.rs.Columns()

	if err != nil {
		r.lastError = err
		return nil, err
	}

	data = make([][]string, 0)

	num := len(fields)

	//根据查询字段的数量，生成[num]interface{}用于存储Scan的结果
	refs := make([]interface{}, num)
	for i := 0; i < num; i++ {
		var ref interface{}
		refs[i] = &ref
	}

	for r.rs.Next() {

		result := make([]string, len(fields))

		if err := r.rs.Scan(refs...); err != nil {
			return nil, err
		}

		for i := range fields {
			//把*interface{}转换成strings返回
			if val, err := toString(refs[i]); err == nil {
				result[i] = val
			} else {
				return nil, err
			}
		}

		if err != nil {
			r.lastError = err
			return nil, err
		}

		data = append(data, result)
	}

	return data, nil
}

func (r *Rows) ToMap() (data []map[string]string, err error) {
	if r.rs == nil {
		return nil, r.lastError
	}

	defer r.rs.Close()

	fields, err := r.rs.Columns()

	if err != nil {
		r.lastError = err
		return nil, err
	}

	data = make([]map[string]string, 0)
	num := len(fields)

	result := make(map[string]string)

	refs := make([]interface{}, num)

	for i := 0; i < num; i++ {
		var ref interface{}
		refs[i] = &ref
	}

	for r.rs.Next() {
		if err := r.rs.Scan(refs...); err != nil {
			return nil, err
		}

		for i, field := range fields {
			if val, err := toString(refs[i]); err == nil {
				result[field] = val
			} else {
				return nil, err
			}
		}

		data = append(data, result)

	}
	return data, nil
}

func (r *Rows) ToStruct(st interface{}) error {
	//st->&[]user
	//获取变量的类型,类型为指针
	stType := reflect.TypeOf(st)

	//获取变量的值
	stVal := reflect.ValueOf(st)
	stValInd := reflect.Indirect(stVal)

	//1.参数必须是指针
	if stType.Kind() != reflect.Ptr {
		return fmt.Errorf("the variable type is %v, not a pointer", stType.Kind())
	}

	//指针指向的类型:slice
	stTypeInd := stType.Elem()
	//2.传入的类型必须是slice,slice的成员类型必须是struct
	if stTypeInd.Kind() != reflect.Slice || stTypeInd.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("the variable type is %v, not a slice struct", stType.Elem().Kind())
	}

	if r.rs == nil {
		return r.lastError
	}

	defer r.rs.Close()

	//初始化struct
	v := reflect.New(stTypeInd.Elem())

	//提取结构体中的tag
	tagList, err := extractTagInfo(v)
	if err != nil {
		return err
	}

	fields, err := r.rs.Columns()

	if err != nil {
		r.lastError = err
		return err
	}

	refs := make([]interface{}, len(fields))

	for i, field := range fields {
		//如果对应的字段在结构体中有映射，则使用结构体成员变量的地址
		if f, ok := tagList[field]; ok {
			refs[i] = f.Addr().Interface()
		} else {
			refs[i] = new(interface{})
		}
	}

	for r.rs.Next() {
		if err := r.rs.Scan(refs...); err != nil {
			return err
		}
		stValInd = reflect.Append(stValInd, v.Elem())
	}

	stVal.Elem().Set(stValInd)

	return nil

}
