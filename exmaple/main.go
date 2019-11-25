/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/21 14:55
 */
package main

import (
	"kdb"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	kConf := new(kdb.KConfig)
	dbConfig := new(kdb.DBConfig)
	dbConfig.Driver = "mysql"
	dbConfig.Dsn = "root:123456@tcp(127.0.0.1:3306)/kdb?charset=utf8&parseTime=true"
	dbConfig.IsMaster = true
	kConf.DBConfigList = []kdb.DBConfig{*dbConfig}
	kdb.RegisterDataBase(*kConf)


	type user struct{
		Id int `db:"id"`
		Name string `db:"name"`
	}
	var result []user
	err := kdb.Select("select * from user").ToStruct(&result)
	if err != nil {
		log.Println("err:", err.Error())
	}

	log.Println("result:", result)
}
