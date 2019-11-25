## 简介

kdb是一个源于Laravel的ORM框架

## 用法示例
```go
package main

import (
	"fmt"
    "kdb"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
    //初始化配置
	kConf := new(kdb.KConfig)
    //初始化DB的配置
	dbConfig := new(kdb.DBConfig)
	dbConfig.Driver = "mysql"
	dbConfig.Dsn = "root:123456@tcp(127.0.0.1:3306)/kdb?charset=utf8&parseTime=true"
	dbConfig.IsMaster = true
	kConf.DBConfigList = []kdb.DBConfig{*dbConfig}
	kdb.RegisterDataBase(*kConf)
    
    //原生SQL查询
    kdb.Select("select * from user where id = ?", 1).ToArray()
    //返回map[string][string]
	kdb.Select("select * from user where id = ?", 1).ToMap()
    //返回struct
    type user struct {
        Id int `db:"id"`
        Name string `db:"string"`
    }
    var result []user
    kdb.Select("select * from user").ToStruct(&result)
    fmt.Println("result:", result)
    
    //链式操作,返回单挑数据
    kdb.Table("user").Where("id", 1).First().ToArray()
}
```