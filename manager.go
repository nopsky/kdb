/**
 * @Author : nopsky
 * @Email : cnnopsky@gmail.com
 * @Date : 2019/11/21 13:59
 */
package kdb

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

const defaultGroupName = "mysql"

var m = newManager()

type manager struct {
	dbs map[string]map[string][]*sql.DB
}

func newManager() *manager {
	m := new(manager)
	m.dbs = make(map[string]map[string][]*sql.DB)
	return m
}

//添加数据库
func (m *manager) addDB(groupName string, isMaster bool, db *sql.DB) {

	dc := "master"
	if !isMaster {
		dc = "slave"
	}

	group, ok := m.dbs[groupName]

	if !ok {
		group = make(map[string][]*sql.DB)
	}

	if _, ok := group[dc]; ok {
		group[dc] = append(group[dc], db)
	} else {
		group[dc] = []*sql.DB{db}
	}

	m.dbs[groupName] = group
}

//获取数据库
func (m *manager) getDB(names ...string) (*sql.DB, error) {
	groupName := defaultGroupName
	dc := "master"

	if len(names) > 0 {
		name := names[0]
		segment := strings.Split(name, "::")
		groupName = segment[0]
		if len(segment) > 1 {
			dc = segment[1]
		}
	}

	if dbs, ok := m.dbs[groupName][dc]; ok {
		max := len(dbs)
		//采用简单的随机获取DB的方式
		rand.Seed(time.Now().UnixNano())
		i := rand.Intn(max)
		return dbs[i], nil
	}

	return nil, fmt.Errorf("DataBase `%s::%s` not found", groupName, dc)
}

//获取从库
func (m *manager) getReadDB(names ...string) (*sql.DB, error) {
	groupName := defaultGroupName
	if len(names) > 0 {
		groupName = names[0]
	}
	name := fmt.Sprintf("%s::%s", groupName, "slave")
	return m.getDB(name)
}
