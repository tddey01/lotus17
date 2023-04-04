package main

import (
	"fmt"
	"github.com/filecoin-project/lotus/storage/db"
	"github.com/urfave/cli/v2"
)

var sectorInfo = &cli.Command{
	Name:  "sectorinfo",
	Usage: "查看重做扇区列表",
	Action: func(c *cli.Context) error {

		sql := `select sector_id,running from ` + db.TableTicket + ` where proving = 1 and running <>2 `
		res, err := db.DBengine.QueryString(sql)
		if err != nil {
			log.Error("修改失败！", err.Error())
			return err
		}
		f1 := true
		f2 := true
		for _, v := range res {
			if v["running"] == "0" {
				if f1 {
					fmt.Println()
					fmt.Println("待重做扇区：")
					f1 = false
				}
				fmt.Print(v["sector_id"], " ")
			}
			if v["running"] == "1" {
				if f2 {
					fmt.Println()
					fmt.Println("正在重做扇区：")
					f2 = false
				}
				fmt.Print(v["sector_id"], " ")
			}
		}
		sql2 := `select sectors,create_time from ` + db.TableRedolog + ` order by id desc limit 0,1 `
		res2, err := db.DBengine.QueryString(sql2)
		if err != nil {
			log.Error("查询失败！", err.Error())
			return err
		}
		if len(res2) > 0 {
			fmt.Println("本次（最后一次）重做时间：", res2[0]["create_time"])
			fmt.Println("本次（最后一次）重做扇区：")
			fmt.Println(res2[0]["sectors"])
		}

		return nil
	},
}

var storageInfo = &cli.Command{
	Name:  "storageinfo",
	Usage: "查看存储列表",
	Action: func(c *cli.Context) error {

		sql := `select path,running from ` + db.TableStorage
		res, err := db.DBengine.QueryString(sql)
		if err != nil {
			log.Error("修改失败！", err.Error())
			return err
		}
		f1 := true
		f2 := true
		for _, v := range res {
			if v["running"] == "1" {
				if f1 {
					fmt.Println("转移中：")
					f1 = false
				}
				fmt.Println(v["path"])
			}
			if v["running"] == "0" {
				if f2 {
					fmt.Println("未使用：")
					f2 = false
				}
				fmt.Println(v["path"])
			}
		}

		return nil
	},
}
