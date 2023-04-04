package main

import (
	"github.com/filecoin-project/lotus/storage/db"
	"github.com/urfave/cli/v2"
	"strings"
)

var storageCmd = &cli.Command{
	Name:  "addstorage",
	Usage: "增加存储",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "move-dirs",
			Usage: "设置转移存储路径逗号隔开：/mnt/md0,/mnt/md1",
		},
	},
	Action: func(c *cli.Context) error {
		//if c.String("sectors-load")==""||c.String("move-dirs")==""{
		//	return xerrors.Errorf("未设置路径：sectors-load、move-dirs")
		//}
		dirs := strings.Split(c.String("move-dirs"), ",")
		sql := `insert into ` + db.TableStorage + `(path) values `
		for i, v := range dirs {
			sql += "('" + v + "')"
			if i < len(dirs)-1 {
				sql += ","
			}
		}
		if _, err := db.DBengine.Exec(sql); err != nil {
			log.Fatal("添加失败！", err.Error())
		}
		return nil
	},
}

var delCmd = &cli.Command{
	Name:  "delstorage",
	Usage: "移除存储",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "move-dirs",
			Usage: "移除转移存储路径逗号隔开：/mnt/md0,/mnt/md1",
		},
	},
	Action: func(c *cli.Context) error {
		//if c.String("sectors-load")==""||c.String("move-dirs")==""{
		//	return xerrors.Errorf("未设置路径：sectors-load、move-dirs")
		//}
		dirs := strings.Split(c.String("move-dirs"), ",")
		for _, v := range dirs {
			sql := `delete from ` + db.TableStorage + ` where path = '` + v + `'`
			if _, err := db.DBengine.Exec(sql); err != nil {
				log.Fatal("添加失败！", err.Error())
			}
		}
		return nil
	},
}

var releaseCmd = &cli.Command{
	Name:  "releasestorage",
	Usage: "释放存储",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "move-dirs",
			Usage: "存储路径逗号隔开：/mnt/md0,/mnt/md1",
		},
	},
	Action: func(c *cli.Context) error {
		//if c.String("sectors-load")==""||c.String("move-dirs")==""{
		//	return xerrors.Errorf("未设置路径：sectors-load、move-dirs")
		//}
		dirs := strings.Split(c.String("move-dirs"), ",")
		for _, v := range dirs {
			sql := `update ` + db.TableStorage + ` set running=0 where path = '` + v + `'`
			if _, err := db.DBengine.Exec(sql); err != nil {
				log.Fatal("添加失败！", err.Error())
			}
		}
		return nil
	},
}
