package main

import (
	"errors"
	"fmt"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	record "github.com/filecoin-project/lotus/extern/record-task"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var workerCmd = &cli.Command{
	Name:  "workers",
	Usage: "worker任务情况",
	Subcommands: []*cli.Command{
		workerInfoCmd,
		workerSetCmd,
		workerDelCmd,
		workerListCmd,
		workerCheckCmd,
		workerUpdateCmd,
	},
	//Action:    schedInfo,
}

var workerInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "查看已发总任务数",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "wid",
			Usage: "查询的workerid",
		},
	},
	Action: func(cctx *cli.Context) error {
		wid := cctx.String("wid")
		if wid == "" {
			return errors.New("wid不能为空！")
		}
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}
		tc, err := nodeApi.WorkerGetTaskCount(ctx, wid)
		if err != nil {
			return err
		}
		ip := ""
		if w, ok := stats[uuid.MustParse(tc.Wid)]; ok {
			ip = w.Info.Ip
		}
		fmt.Println("workerid：", tc.Wid)
		fmt.Println("ip------：", ip)
		fmt.Println("AP------：", tc.APcount)
		fmt.Println("P1------：", tc.P1count)
		return nil
	},
}
var workerCheckCmd = &cli.Command{
	Name:  "check",
	Usage: "查看记录与sealing jobs不符机器",

	Action: func(cctx *cli.Context) error {

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		jbs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return err
		}
		tl, err := nodeApi.WorkerGetTaskList(ctx)
		if err != nil {
			return err
		}
		jbsmap := make(map[string]record.TaskCount)

		for k, js := range jbs {
			var v record.TaskCount
			onrunnig:
			for _, jv := range js {
				if jv.RunWait!=0  {
					break onrunnig
				}
				switch jv.Task {
				case sealtasks.TTAddPiece:
					v.APcount++
				case sealtasks.TTPreCommit1:
					v.P1count++
					v.APcount++
				default:
					break onrunnig
				}
			}
			if v != (record.TaskCount{}){
				jbsmap[k.String()] = v
			}
		}
		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}
		for _, v := range tl {
			if _, ok := stats[uuid.MustParse(v.Wid)]; !ok {
				continue
			}
			if _, ok := jbsmap[v.Wid]; !ok {
				continue
			}
			if v.P1count != jbsmap[v.Wid].P1count {
				fmt.Println("workerid：", v.Wid, ",Ip:", stats[uuid.MustParse(v.Wid)].Info.Ip)
				fmt.Println("AP------：", v.APcount, ",jbs------：", jbsmap[v.Wid].APcount)
				fmt.Println("P1------：", v.P1count, ",jbs------：", jbsmap[v.Wid].P1count)
			}
		}

		return nil
	},
}
var workerUpdateCmd = &cli.Command{
	Name:  "update",
	Usage: "更新记录与sealing jobs不符机器",

	Action: func(cctx *cli.Context) error {

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		jbs, err := nodeApi.WorkerJobs(ctx)
		if err != nil {
			return err
		}
		tl, err := nodeApi.WorkerGetTaskList(ctx)
		if err != nil {
			return err
		}
		jbsmap := make(map[string]record.TaskCount)
		for k, js := range jbs {
			var v record.TaskCount
			onrunnig:
				for _, jv := range js {
					if jv.RunWait!=0  {
						break onrunnig
					}
					switch jv.Task {
					case sealtasks.TTAddPiece:
						v.APcount++
					case sealtasks.TTPreCommit1:
						v.P1count++
						v.APcount++
					default:
						break onrunnig
					}
				}
			if v != (record.TaskCount{}){
				jbsmap[k.String()] = v
			}
		}
		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}
		for _, v := range tl {
			if _, ok := stats[uuid.MustParse(v.Wid)]; !ok {
				continue
			}
			if _, ok := jbsmap[v.Wid]; !ok {
				continue
			}
			if v.P1count != jbsmap[v.Wid].P1count{
				if v.APcount <= v.P1count{
					v.APcount = jbsmap[v.Wid].P1count
					v.P1count = jbsmap[v.Wid].P1count
				}
				if err = nodeApi.WorkerSetTaskCount(ctx, v); err != nil {
					return err
				}
				fmt.Println("设置成功：",v.Wid,",",stats[uuid.MustParse(v.Wid)].Info.Ip,",ap:",v.APcount,"p1:",v.P1count)
			}
		}

		return nil
	},
}
var workerListCmd = &cli.Command{
	Name:  "list",
	Usage: "查看已发总任务数",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := lcli.ReqContext(cctx)
		ts, err := nodeApi.WorkerGetTaskList(ctx)
		if err != nil {
			return err
		}
		stats, err := nodeApi.WorkerStats(ctx)
		if err != nil {
			return err
		}
		for _, v := range ts {
			ip := ""
			if w, ok := stats[uuid.MustParse(v.Wid)]; ok {
				ip = w.Info.Ip
			}
			fmt.Println("-------------------------------------------------------------")
			fmt.Println("workerid：", v.Wid)
			fmt.Println("ip------：", ip)
			fmt.Println("AP------：", v.APcount)
			fmt.Println("P1------：", v.P1count)
		}
		return nil
	},
}
var workerDelCmd = &cli.Command{
	Name:  "del",
	Usage: "删除worker上任务数",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "wid",
			Usage: "查询的workerid",
		},
		&cli.BoolFlag{
			Name:  "all",
			Usage: "删掉全部worker记录任务信息",
		},
	},
	Action: func(cctx *cli.Context) error {
		wid := cctx.String("wid")
		if wid == "" && !cctx.Bool("all") {
			return errors.New("wid不能为空！")
		}
		ctx := lcli.ReqContext(cctx)
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		if wid != "" {

			err = nodeApi.WorkerDelTaskCount(ctx, wid)
			if err != nil {
				return err
			}
			return nil
		}
		if cctx.Bool("all") {
			//ctx, cancel := context.WithCancel(ctx)
			//defer cancel()
			//repoPath := cctx.String(FlagMinerRepo)
			//r, err := repo.NewFS(repoPath)
			//if err != nil {
			//	return err
			//}
			//lr, err := r.NoLock(repo.StorageMiner)
			//if err != nil {
			//	return err
			//}
			//mds, err := lr.Datastore(ctx, "/metadata")
			//if err != nil {
			//	return err
			//}
			//tcalls := statestore.New(namespace.Wrap(mds, modules.TaskPrefix))
			//
			////tcalls.ds.Query(query.Query{})
			//if err = tcalls.DeleteList(); err != nil {
			//	log.Error("错误信息：", err.Error())
			//	return err
			//}
			return nodeApi.WorkerDelAll(ctx)
		}
		return nil
	},
}

var workerSetCmd = &cli.Command{
	Name:  "setting",
	Usage: "设置已发任务总数",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "wid",
			Usage: "设置的workerid",
		},
		&cli.Int64Flag{
			Name:  "ap",
			Usage: "设置AP任务数",
		},
		&cli.Int64Flag{
			Name:  "p1",
			Usage: "设置P1任务数",
		},
		&cli.Int64Flag{
			Name:  "p2",
			Usage: "设置P2任务数",
		},
		&cli.Int64Flag{
			Name:  "c2",
			Usage: "设置C2任务数",
		},
	},
	Action: func(cctx *cli.Context) error {
		wid := cctx.String("wid")
		if wid == "" {
			return errors.New("wid不能为空！")
		}
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)

		mp := make(map[string]bool)
		if cctx.IsSet("ap") {
			mp["ap"] = true
		}
		if cctx.IsSet("p1") {
			mp["p1"] = true
		}
		if cctx.IsSet("p2") {
			mp["p2"] = true
		}
		if cctx.IsSet("c2") {
			mp["c2"] = true
		}
		if len(mp) == 0 {
			return errors.New("输入参数有误")
		}
		tc, err := nodeApi.WorkerGetTaskCount(ctx, wid)
		if err != nil {
			return err
		}
		for k, v := range mp {
			switch k {
			case "ap":
				if v {
					tc.APcount = int(cctx.Int64("ap"))
				}
			case "p1":
				if v {
					tc.P1count = int(cctx.Int64("p1"))
				}
			}
		}
		if err = nodeApi.WorkerSetTaskCount(ctx, tc); err != nil {
			return err
		}

		tc1, err := nodeApi.WorkerGetTaskCount(ctx, wid)
		if err != nil {
			return err
		}

		fmt.Println("设置成功workerid：", tc1.Wid)
		fmt.Println("AP-------------：", tc1.APcount)
		fmt.Println("P1-------------：", tc1.P1count)

		return nil
	},
}

