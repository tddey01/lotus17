package main

import (
	"errors"
	"github.com/filecoin-project/lotus/extern/server-c2"
	"github.com/urfave/cli/v2"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"

	"github.com/filecoin-project/lotus/build"
)

func main() {

	log.Println("Starting lotus-commit2")

	app := &cli.App{
		Name:    "lotus-commit2",
		Usage:   "Benchmark performance of lotus on your hardware",
		Version: build.UserVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "host",
				Value: "9528",
				Usage: "监听端口",
			},
			&cli.StringFlag{
				Name:  "url",
				Value: "",
				Usage: "分发服务地址 127.0.0.1:9527",
			},
			&cli.StringFlag{
				Name:  "pathc1",
				Value: server_c2.PATHC1,
				Usage: "C1参数暂存路径",
			},
			&cli.StringFlag{
				Name:  "pathc2",
				Value: server_c2.PATHC2,
				Usage: "结果暂存路径",
			},
		},
		Action: func(c *cli.Context) error {
			server_c2.PATHC2 = c.String("pathc2")
			_ = os.MkdirAll(server_c2.PATHC2,0664)
			server_c2.PATHC1 = c.String("pathc1")
			_ = os.MkdirAll(server_c2.PATHC1,0664)
			if server_c2.PATHC1 == server_c2.PATHC2{
				log.Fatal("两个地址不能一样!")
			}
			if os.Getenv("C2_TOKEN")==""{
				log.Fatal("C2_TOKEN:不能为空" )
			}

			host := c.String("host")
			sock, err := net.Listen("tcp", ":"+host)
			if err != nil {
				log.Fatal("listen error:" + err.Error())
			}
			URL := c.String("url")
			if URL == "" {
				return errors.New("请设置url")
			}
			CInfo.Url = URL
			CInfo.Host = host

			if err := CInfo.ReturnCommit2();err!=nil{
				log.Fatal("清除任务失败：",err.Error())
			}
			if err := rpc.Register(new(Sealer)); err != nil {
				return err
			}
			go func() {
				time.Sleep(time.Second)
				CInfo.httpGet()
				//webget()
			}()

			return CInfo.Accept(sock)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Println("%+v", err)
		return
	}
}

