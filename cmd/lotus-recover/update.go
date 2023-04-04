package main

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/lotus/storage/db"
	"github.com/urfave/cli/v2"
	"strconv"
	"strings"
)

var updateCmd = &cli.Command{
	Name:  "start",
	Usage: "重做扇区列表，重做此次设置部分（覆盖上次设置）",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "sids",
			Usage: "扇区ID列表：'0 1 2 3'（all代表全部）",
		},
	},
	Action: func(c *cli.Context) error {
		sids := c.String("sids")
		if sids == "" {
			return errors.New("sids不能为空")
		}

		sess := db.DBengine.NewSession()
		//if _,err := sess.Exec("commit");err!=nil{
		//	log.Error("提交事务失败！",err.Error())
		//}
		var err error
		sql2 := `update ` + db.TableTicket + ` set running = 0,redo_time=now() where proving = 1 `
		if sids != "all" {
			sql1 := `update ` + db.TableTicket + ` set running = 2 where proving = 1 and running <> 2 `
			if _, err = sess.Exec(sql1); err != nil {
				sess.Rollback()
				log.Error("添加失败！", err.Error())
				return err
			}

			sid := strings.Split(sids, " ")
			str := ""
			for index, v := range sid {
				if _, err = strconv.Atoi(v); err != nil {
					sess.Rollback()
					err = errors.New("扇区号非法!")
					return err
				}
				str += v
				if index < len(sid)-1 {
					str += ","
				}
			}
			if str == "" {
				sess.Rollback()
				err = errors.New("扇区号非法!")
				return err
			}
			sql2 += ` and sector_id in (` + str + `) `
		}

		if _, err = sess.Exec(sql2); err != nil {
			sess.Rollback()
			log.Error("修改失败！", err.Error())
			return err
		}
		sql3 := `insert into ` + db.TableRedolog + ` (sectors) value('` + sids + `')`
		if _, err = sess.Exec(sql3); err != nil {
			sess.Rollback()
			log.Error("保存失败！", err.Error())
			return err
		}
		sess.Commit()
		sql := `select sector_id from ` + db.TableTicket + ` where proving = 1 and running = 0 `
		res, err := db.DBengine.QueryString(sql)
		if err != nil {
			log.Error("修改失败！", err.Error())
			return err
		}
		fmt.Println("准备重做扇区：")
		for _, v := range res {
			fmt.Print(v["sector_id"], " ")
		}
		fmt.Println("")
		return nil
	},
}
