package db

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Mysql struct {
	DBname     string
	DBhost     string
	DBusername string
	DBpassword string
	DBtable    string
}

const PledgeFile = "pledgeconfig.json"

var DBengine *xorm.Engine
var TableTicket string
var TableStorage string
var TableRedolog string
var TableStore string
var TablePiece string
var Table string

const (
	STORAGE      = "_storage"
	TICKET       = "_ticket"
	REDOLOG      = "_redolog"
	STOREMACHINE = "_store_machine"
	PIECE        = "_piece"
)

func NewEngine() {
	var MysqlConf Mysql
	//log.Info("读取配置文件：", filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile))
	file, err := ioutil.ReadFile(filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile))
	if err != nil {
		log.Info("数据库错误：打开配置文件失败", err.Error())
		return
	}
	if err = json.Unmarshal(file, &MysqlConf); err != nil {
		log.Info("数据库错误：数据解析失败！", err.Error())
		return
	}
	if DBengine, err = xorm.NewEngine("mysql", fmt.Sprintf("%s:%s@(%s)/%s?charset=utf8", MysqlConf.DBusername, MysqlConf.DBpassword, MysqlConf.DBhost, MysqlConf.DBname)); err != nil {
		log.Info("数据库错误：链接数据库失败！", err.Error())
		return
	}
	Table = MysqlConf.DBtable
	TableTicket = MysqlConf.DBtable + TICKET

	TableStorage = MysqlConf.DBtable + STORAGE

	TableRedolog = MysqlConf.DBtable + REDOLOG

	TableStore = MysqlConf.DBtable + STOREMACHINE

	TablePiece = MysqlConf.DBtable + PIECE
	sql := `
        CREATE TABLE IF NOT EXISTS ` + TableTicket + `  (
            id bigint(20) NOT NULL AUTO_INCREMENT,
            miner_id varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
            sector_id bigint(20) NULL DEFAULT NULL,
            ticket varchar(70) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
            ticket_h int(11) NULL DEFAULT NULL,
            create_time datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
            proving int(11) NULL DEFAULT 0 COMMENT '是否完成扇区 0未完成，1已完成',
            running int(11) NULL DEFAULT 2 COMMENT '0未开始，1进行中，2已完成',
            redo_time datetime(0) NULL DEFAULT NULL,
            cid_commd varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT 'CIDcommD',
            size varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '扇区大小字节',
            data varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
            cid_commr varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
            precommit varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
            commit varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
            PRIMARY KEY (id) USING BTREE,
            UNIQUE INDEX SECTOR_ID_UNIQUE(sector_id) USING BTREE,
            INDEX SECTOR_ID(sector_id) USING BTREE
          ) ENGINE = InnoDB AUTO_INCREMENT = 17 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic`
	if _, err = DBengine.Exec(sql); err != nil {
		log.Info("数据库错误：创建表失败！", err.Error())
		return
	}
	sql2 := `
	CREATE TABLE IF NOT EXISTS ` + TableStorage + `  (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		path varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
		running int(11) NULL DEFAULT 0 COMMENT '0空闲，1正在使用，2已满',
		create_time datetime NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id) USING BTREE,
		UNIQUE INDEX PATH(path) USING BTREE
	) ENGINE = InnoDB AUTO_INCREMENT = 2 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;`

	if _, err = DBengine.Exec(sql2); err != nil {
		log.Info("数据库错误：创建表失败！", err.Error())
		return
	}
	sql3 := `
	CREATE TABLE IF NOT EXISTS ` + TableRedolog + `  (
		id bigint(20) NOT NULL AUTO_INCREMENT,
    	sectors longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '重做列表',
    	create_time datetime NULL DEFAULT CURRENT_TIMESTAMP,
    	PRIMARY KEY (id) USING BTREE
	) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;`

	if _, err = DBengine.Exec(sql3); err != nil {
		log.Info("数据库错误：创建表失败！", err.Error())
		return
	}
	sql4 := `
	CREATE TABLE IF NOT EXISTS ` + TableStore + `  (
	   id bigint(20) NOT NULL AUTO_INCREMENT,
	   address varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '存储机IP',
	   enable int(11) NULL DEFAULT 1 COMMENT '0禁用，1启用',
	   create_time datetime NULL DEFAULT CURRENT_TIMESTAMP,
	   PRIMARY KEY (id) USING BTREE,
       UNIQUE INDEX ADDRESS(address) USING BTREE
	) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;
	`
	if _, err = DBengine.Exec(sql4); err != nil {
		log.Info("数据库错误：创建表失败！", err.Error())
		return
	}
	sql5 := `
	CREATE TABLE IF NOT EXISTS ` + TablePiece + `  (
	   id bigint(20) NOT NULL AUTO_INCREMENT,
	   piece_cid varchar(70) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
	   deal_id int(12) NULL DEFAULT NULL,
	   piece_size bigint(20) NULL DEFAULT NULL,
	   sector_id bigint(20) NULL DEFAULT NULL,
	   miner_id varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
	   create_time datetime NULL DEFAULT CURRENT_TIMESTAMP,
	   PRIMARY KEY (id) USING BTREE,
	   INDEX SECTOR_ID(sector_id) USING BTREE,
	   INDEX MINER_ID(miner_id) USING BTREE,
	   UNIQUE INDEX PIECE_CID_AND_SECTOR_ID_UNIQUE(piece_cid, sector_id) USING BTREE
	) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
	`
	if _, err = DBengine.Exec(sql5); err != nil {
		log.Info("数据库错误：创建表失败！", err.Error())
		return
	}
	//log.Println("数据库连接成功")
	DBengine.ShowSQL(false)
}
