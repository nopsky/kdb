/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/21 13:59
 */
package kdb

import "time"

type DBConfig struct {
	Name         string //数据库连接别名
	IsMaster     bool   //是否是主库
	Driver       string
	Dsn          string
	MaxLifetime  time.Duration
	MaxIdleConns int
	MaxOpenConns int
}

type KConfig struct {
	DBConfigList []DBConfig
}
