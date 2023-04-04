package main

import (
	"errors"
	"fmt"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
)

var schedCmd = &cli.Command{
	Name:  "sched",
	Usage: "查看任务数",
	Subcommands: []*cli.Command{
		schedInfoCmd,
		schedSetCmd,
	},
	//Action:    schedInfo,
}

var schedInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "查看已发总任务数",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		num, err := nodeApi.SchedAlreadyIssueInfo(ctx)
		if err != nil {
			return err
		}
		fmt.Println("已发任务总数为：", num)
		return nil
	},
}

var schedSetCmd = &cli.Command{
	Name:  "setting",
	Usage: "设置已发总任务数",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:  "sub",
			Usage: "减少任务总量",
		},
		&cli.Int64Flag{
			Name:  "add",
			Usage: "增加任务总量",
		},
		&cli.Int64Flag{
			Name:  "set",
			Usage: "设置任务总量",
		},
	},
	Action: func(cctx *cli.Context) error {

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		mp := make(map[string]int64)
		if cctx.IsSet("sub") {
			mp["sub"] = cctx.Int64("sub")
		}
		if cctx.IsSet("add") {
			mp["add"] = cctx.Int64("add")
		}
		if cctx.IsSet("set") {
			mp["set"] = cctx.Int64("set")
		}
		if len(mp) > 1 {
			return errors.New("输入参数有误")
		}
		for k, v := range mp {
			switch k {
			case "sub":
				err = nodeApi.SchedSubAlreadyIssue(ctx, v)
			case "add":
				err = nodeApi.SchedAddAlreadyIssue(ctx, v)
			case "set":
				err = nodeApi.SchedSetAlreadyIssue(ctx, v)
			}
		}
		if err != nil {
			return err
		}
		num, err := nodeApi.SchedAlreadyIssueInfo(ctx)
		if err != nil {
			return err
		}
		fmt.Println("设置成功！现在为：", num)

		return nil
	},
}
