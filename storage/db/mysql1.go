package db

import (
	"bytes"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql" //加载mysql
	logging "github.com/ipfs/go-log/v2"
	"github.com/jinzhu/gorm"
	"io/ioutil"
	"os"
	"path/filepath"
)

var log = logging.Logger("mysql")
var (
	Eloquent *gorm.DB
)

func init() {
	//initAdmin()
}

//初始化管理系统数据库链接
func initAdmin() {
	file, err := ioutil.ReadFile(filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile))
	if err != nil {
		log.Info("数据库错误：打开配置文件失败", err.Error())
		return
	}
	var MysqlConf Mysql2

	if err = json.Unmarshal(file, &MysqlConf); err != nil {
		log.Info("数据库错误：数据解析失败！", err.Error())
		return
	}

	conn, dbType := Mysqlconn("admin", MysqlConf)
	log.Debug("管理系统数据库链接：" + conn)
	var db Database
	if dbType == "mysql" {
		db = new(Mysql1)
	} else {
		panic("db type unknow")
	}

	Eloquent, err = db.Open(dbType, conn)
	Eloquent.LogMode(true)
	if err != nil {
		log.Fatal("mysql admin connect error %v", err)
	} else {
		log.Debug("mysql admin connect success!")
	}
	if Eloquent.Error != nil {
		log.Fatal("database error %v", Eloquent.Error)
	}
	TableTicket = MysqlConf.DBtable + TICKET

	TableStorage = MysqlConf.DBtable + STORAGE

	TableRedolog = MysqlConf.DBtable + REDOLOG

	TableStore = MysqlConf.DBtable + STOREMACHINE

}

type Mysql2 struct {
	DBname     string
	DBhost     string
	DBusername string
	DBpassword string
	DBtable    string
}

//数据库链接
func Mysqlconn(typesql string, mysqlconf Mysql2) (conns string, dbType string) {

	var host, database, username, password string

	switch typesql {
	case "center":
		dbType = "mysql"
		host = mysqlconf.DBhost
		database = mysqlconf.DBname
		username = mysqlconf.DBusername
		password = mysqlconf.DBpassword
	case "admin":
		dbType = "mysql"
		host = mysqlconf.DBhost
		database = mysqlconf.DBname
		username = mysqlconf.DBusername
		password = mysqlconf.DBpassword
	}

	var conn bytes.Buffer
	conn.WriteString(username)
	conn.WriteString(":")
	conn.WriteString(password)
	conn.WriteString("@tcp(")
	conn.WriteString(host)
	conn.WriteString(")")
	conn.WriteString("/")
	conn.WriteString(database)
	conn.WriteString("?charset=utf8&parseTime=true&loc=Local&timeout=5s")
	conns = conn.String()
	return
}

type Database interface {
	Open(dbType string, conn string) (db *gorm.DB, err error)
}

type Mysql1 struct {
}

func (*Mysql1) Open(dbType string, conn string) (db *gorm.DB, err error) {
	eloquent, err := gorm.Open(dbType, conn)
	return eloquent, err
}

type SqlLite struct {
}

func (*SqlLite) Open(dbType string, conn string) (db *gorm.DB, err error) {
	eloquent, err := gorm.Open(dbType, conn)
	return eloquent, err
}
