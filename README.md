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
        Id int `db:"id;auto"`
        Name string `db:"string"`
    }
    var result []user
    kdb.Select("select * from user").ToStruct(&result)
    fmt.Println("result:", result)
    
    //链式操作,返回单条数据
    kdb.Table("user").Where("id", 1).First().ToArray()
    
    //支持指定库操作
    var u user
    kdb.WithDB("mysql::master").Table("user").Where("id", 1).First().ToStruct(&u)
    
    //批量插入支持map方式和struct方式
    a1 := new(user)
    a1.Name = "张三"
    
    a2 := new(user)
    a2.Name = "李四"
    
    users := []user{*a1, *a2}
    kdb.Table("user").MultiInsert(users)
}
```