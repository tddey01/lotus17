package main

import (
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/filecoin-project/lotus/extern/server-c2"
	"github.com/urfave/cli/v2"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {

	app := &cli.App{
		Name:  "lotus-cservice",
		Usage: "C2代理服务",
		Commands: []*cli.Command{
			run,
			token,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err.Error())
	}
}

//func ListeningClientOnline(){
//	var tcpAddr *net.TCPAddr
//}

var run = &cli.Command{
	Name:  "run",
	Usage: "Start cservice",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Value: "9527",
			Usage: "监听端口",
		},
		&cli.StringFlag{
			Name:  "pathc1",
			Value: server_c2.CSPATHC1,
			Usage: "记录正在运行扇区路径",
		},
		&cli.StringFlag{
			Name:  "pathc2",
			Value: server_c2.CSPATHC2,
			Usage: "结果暂存路径",
		},
	},
	Action: func(c *cli.Context) error {
		server_c2.CSPATHC2 = c.String("pathc1")
		_ = os.MkdirAll(server_c2.CSPATHC1, 0664)
		server_c2.CSPATHC2 = c.String("pathc2")
		_ = os.MkdirAll(server_c2.CSPATHC2, 0664)
		//读取已完成
		if err := AddCommit2(); err != nil {
			log.Fatal(err.Error())
		}
		return Server(c.String("listen"))
	},
}

var token = &cli.Command{
	Name:  "token",
	Usage: "create token",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "mid",
			Value: "",
			Usage: "矿工ID",
		},
		&cli.StringFlag{
			Name:  "key",
			Value: TOKEN_KEY,
			Usage: "自定义密钥默认",
		},
		&cli.Int64Flag{
			Name:  "day",
			Value: 36600,
			Usage: "token有效时间天数",
		},
	},
	Action: func(c *cli.Context) error {
		//创建token
		mid := c.String("mid")
		if mid == "" {
			return errors.New("矿工ID不能为空！")
		}
		tokenstr, err := Sign(mid, c.Int64("day"), c.String("key"))
		if err != nil {
			return err
		}
		fmt.Println(tokenstr)
		return nil
	},
}

//Sing签名生成token字符串
func Sign(mid string, day int64, key string) (string, error) {
	token := jwt.New(jwt.GetSigningMethod("HS256"))
	claims := token.Claims.(jwt.MapClaims)
	claims["exp"] = time.Now().Add(TOKEN_EFFECT_TIME * time.Duration(day)).Unix()
	claims["miner_id"] = mid

	return token.SignedString([]byte(key))
}

func AddCommit2() error {
	//返回已完成
	filepath.Walk(server_c2.CSPATHC1, func(path string, info fs.FileInfo, err error) error {
		if info == nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if server_c2.CheckSectorNum(info.Name()) {
			log.Println("正在运行：", info.Name())
			RunningRL.Lock()
			Running[server_c2.StringToSectorID(info.Name())] = time.Now()
			RunningRL.Unlock()

			return nil
		}
		return nil
	})

	return filepath.Walk(server_c2.CSPATHC2, func(path string, info fs.FileInfo, err error) error {
		if info == nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if server_c2.CheckSectorNum(info.Name()) {
			path := filepath.Join(server_c2.CSPATHC2, info.Name())
			proof, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			log.Println("已有结果：", info.Name())
			Commit2RL.Lock()
			Commit2[server_c2.StringToSectorID(info.Name())] = &server_c2.ProofResult{proof, ""}
			Commit2RL.Unlock()

			return nil
		}
		return nil
	})
}
