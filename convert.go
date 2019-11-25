/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/25 11:31
 */
package kdb

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)


//转换成string
func toString(src interface{}) (dst string, err error) {
	inf := reflect.Indirect(reflect.ValueOf(src)).Interface()
	if inf == nil {
		return "", nil
	}

	switch v := inf.(type) {
	case string:
		dst = v
		return
	case []byte:
		dst = string(v)
		return
	}

	val := reflect.ValueOf(inf)
	typ := reflect.TypeOf(inf)

	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst = strconv.FormatInt(val.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dst = strconv.FormatUint(val.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		dst = strconv.FormatFloat(val.Float(), 'f', -1, 64)
	case reflect.Bool:
		dst = strconv.FormatBool(val.Bool())
	case reflect.Complex64, reflect.Complex128:
		dst = fmt.Sprintf("%v", val.Complex())
	case reflect.Struct:
		//time.Time
		var timeType time.Time
		if typ.ConvertibleTo(reflect.TypeOf(timeType)) {
			dst = val.Convert(reflect.TypeOf(timeType)).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("unsupported struct type %v", val.Type())
		}
	default:
		err = fmt.Errorf("unsupported struct type %v", val.Type())
	}

	return
}

//提取tag信息
func extractTagInfo(st reflect.Value) (tagList map[string]reflect.Value, err error) {

	stVal := reflect.Indirect(st)

	if stVal.Kind() != reflect.Struct {
		return nil, fmt.Errorf("the variable type is %v, not a struct", stVal.Kind())
	}

	tagList = make(map[string]reflect.Value)

	for i := 0; i < stVal.NumField(); i++ {

		//获取结构体成员
		v := stVal.Field(i)

		if v.Kind() == reflect.Ptr {
			//如果没有初始化，则需要先初始化
			if v.IsNil() {
				var typ reflect.Type
				if v.Type().Kind() == reflect.Ptr {
					typ = v.Type().Elem()
				} else {
					typ = v.Type()
				}
				vv := reflect.New(typ)
				v.Set(vv)
			}
			//如果是结构体指针，则在进行提取
			if v.Elem().Kind() == reflect.Struct {
				t, err := extractTagInfo(v.Elem())
				if err != nil {
					return nil, err
				}

				for k, ptr := range t {
					if _, ok := tagList[k]; ok {
						return nil, fmt.Errorf("%s:%s is exists", "db", k)
					}

					tagList[k] = ptr
				}
			}
		} else if v.Kind() == reflect.Map && v.IsNil() {
			//如果是map类型，并且没有初始化，则需要初始化一下
			v.Set(reflect.MakeMap(v.Type()))
		} else if v.Kind() == reflect.Struct {
			var ignore bool
			//以下的类型，会再scan的执行转换，所以不需要二次处理
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
				t, err := extractTagInfo(v)
				if err != nil {
					return nil, err
				}

				for k, ptr := range t {
					if _, ok := tagList[k]; ok {
						return nil, fmt.Errorf("%s:%s is exists", "db", k)
					}
					tagList[k] = ptr
				}
			}
		}

		tagName := stVal.Type().Field(i).Tag.Get("db")
		if tagName != "" {
			//tag内容通过";"进行分割
			attr := strings.Split(tagName, ";")
			column := attr[0]
			if _, ok := tagList[column]; ok {
				return nil, fmt.Errorf("%s:%s is exists", "db", tagName)
			}
			//字段对应结构体成员地址
			tagList[column] = v
		}
	}

	return
}