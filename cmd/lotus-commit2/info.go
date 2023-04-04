package main

import (
	"encoding/json"
	"fmt"
	server_c2 "github.com/filecoin-project/lotus/extern/server-c2"
	"io/fs"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
var CInfo Commit2Info
type Commit2Info struct {
	Url string
	Gpu bool
	Host string
	InfoRL sync.RWMutex
	Token string
}

//连接分发服务
func (c *Commit2Info)httpGet() {
	for {
		client := &http.Client{
			Timeout: time.Second * 15,
		}
		//param := make(map[string]string)
		//param["host"] = host
		//rejson.Marshal(param)
		//param := url.Values{"host":{host}}
		param := make(server_c2.Param)
		c.InfoRL.RLock()
		param["host"] = CInfo.Host
		param["gpu"] = CInfo.Gpu
		c.InfoRL.RUnlock()
		path := "http://" + c.Url + "/connectserver"
		//url += path + "&host=" + host
		req, err := http.NewRequest("POST", path, param.Marshal())
		if err != nil {
			fmt.Printf("Error creating request: %v\n", err)
			return
		}
		// 设置header
		req.Header.Add("Content-Type", "application/json")
		req.Header.Add("Content-Length", strconv.FormatInt(req.ContentLength, 10))
		req.Header.Add("token", os.Getenv("C2_TOKEN"))
		// 在这里开始进行请求
		if _, err := client.Do(req);err != nil {
			log.Println("请求错误：", err.Error())
			//return
		}

		//var r server_c2.Respones
		//json.Unmarshal(body, &r)
		//var pr server_c2.ProofResult
		//b,_ := json.Marshal(r.Data)
		//json.Unmarshal(b,&pr)
		//fmt.Println(string(pr.Proof))

		time.Sleep(time.Second*15)
	}
}

//处理
func (c *Commit2Info)Accept(sock net.Listener) error {
	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Println("处理失败：", err.Error())
			return err
		}
		go jsonrpc.ServeConn(conn)
	}
}

//处理未处理任务
func (c *Commit2Info)ReturnCommit2() error {
	//返回已完成
	if err := filepath.Walk(server_c2.PATHC2, func(path string, info fs.FileInfo, err error) error {
		if info==nil{
			return err
		}
		if info.IsDir(){
			return nil
		}
		if server_c2.CheckSectorNum(info.Name()){
			path := filepath.Join(server_c2.PATHC2,info.Name())
			proof,err := ioutil.ReadFile(path)
			if err!=nil{
				return err
			}
			sc := server_c2.SectorIDCommit2{
				server_c2.StringToSectorID(info.Name()),
				server_c2.ProofResult{proof,""},
			}
			for {
				var rep server_c2.Respones
				if err := server_c2.RequestDo(c.Url,"/completecommit2", &sc, &rep,time.Second*15); err != nil {
					log.Println(err.Error())
					time.Sleep(time.Second*10)
					continue
				}
				if rep.Code != http.StatusOK{
					continue
				}
				break
			}
			return os.Remove(filepath.Join(server_c2.PATHC2,info.Name()))
		}
		return nil
	});err!=nil{
		return err
	}
	//返回未完成
	return filepath.Walk(server_c2.PATHC1, func(path string, info fs.FileInfo, err error) error {
		if info==nil{
			return err
		}
		if info.IsDir(){
			return nil
		}
		if server_c2.CheckSectorNum(info.Name()){
			path := filepath.Join(server_c2.PATHC1,info.Name())
			proof,err := ioutil.ReadFile(path)
			if err!=nil{
				return err
			}
			var args server_c2.SealerParam
			if err := json.Unmarshal(proof,&args);err!=nil{
				return err
			}

			for {
				var rep server_c2.Respones
				if err := server_c2.RequestDo(c.Url,"/runcommit2", &args, &rep,time.Second*15); err != nil {
					log.Println(err.Error())
					time.Sleep(time.Second*10)
					continue
				}
				break
			}
			return os.Remove(filepath.Join(server_c2.PATHC1,info.Name()))
		}
		return nil
	})
}


