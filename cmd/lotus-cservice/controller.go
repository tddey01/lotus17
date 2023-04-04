package main

import (
	"encoding/json"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/server-c2"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//func CheckCommit2(w http.ResponseWriter,r *http.Request){
//	_ = checkHandle(w,r)
//}
func RunCommit2(ctx *Context) {
	var sp server_c2.SealerParam
	if err := sp.Unmarshal(ctx.request.Body); err != nil {
		log.Println("解析错误：", ctx.request.Body, err.Error())
		ctx.Result(400, err.Error())
		return
	}
	log.Println("sp:", sp.Sector.ID.Miner, sp.Sector.ID.Number)
	Commit2RL.RLock()
	if _, ok := Commit2[sp.Sector.ID]; ok {
		Commit2RL.RUnlock()
		log.Println("任务已完成!", sp.Sector.ID.Miner, sp.Sector.ID.Number)
		ctx.Result(http.StatusCreated, "任务已完成")
		return
	}
	Commit2RL.RUnlock()
	if sp.Status == 0 {
		RunningRL.RLock()
		if t, ok := Running[sp.Sector.ID]; ok && t.Add(time.Hour).Before(time.Now()) {
			RunningRL.RUnlock()
			log.Println("扇区正在跑!", sp.Sector.ID.Miner, sp.Sector.ID.Number)
			ctx.Result(http.StatusOK, "扇区正在跑")
			return
		}
		RunningRL.RUnlock()
	}

	WorkersRL.Lock()
	defer WorkersRL.Unlock()
	for k, v := range WorkersClient {
		if !v.GpuUse && !v.Disconnect {
			v.WorkerRL.Lock()
			err := v.RunCommit2(sp)
			if err != nil {
				v.WorkerRL.Unlock()
				log.Println("错误信息：", k, err.Error())
				ctx.Result(400, err.Error())
				return
			}
			v.GpuUse = true
			//保存running
			RunningRL.Lock()
			Running[sp.Sector.ID] = time.Now()
			file, _ := os.OpenFile(filepath.Join(server_c2.CSPATHC1, server_c2.SectorNumString(sp.Sector.ID)), os.O_CREATE|os.O_WRONLY, 0664)
			file.Close()
			RunningRL.Unlock()
			log.Println("开始任务：", sp.Sector.ID.Miner, sp.Sector.ID.Number, ",commit2机器：", k)
			v.WorkerRL.Unlock()
			ctx.Result(http.StatusOK, "发送成功！")
			return
		}
	}
	log.Println("任务已满：", sp.Sector.ID.Miner, sp.Sector.ID.Number)
	ctx.Result(400, "任务已满")
	return
}
func GetCommit2(ctx *Context) {
	var s abi.SectorID
	//if err := s.UnmarshalCBOR(ctx.request.Body); err != nil {
	//	log.Println("解析错误：", err.Error())
	//	ctx.Result(400, err.Error())
	//	return
	//}
	body, err := ioutil.ReadAll(ctx.request.Body)
	if err != nil {
		log.Println("GetCommit2读取错误：", err.Error())
		ctx.Result(400, err.Error())
		return
	}
	if err := json.Unmarshal(body, &s); err != nil {
		log.Println("GetCommit2解析错误：", err.Error())
		ctx.Result(400, err.Error())
		return
	}
	//if ctx.Get("mid").(string) != s.Miner.String() {
	//	log.Println("无权限操作该miner:", ctx.Get("mid").(string), "-", s.Miner.String())
	//	ctx.Result(500, errors.New("无权限操作该miner"))
	//	return
	//}
	Commit2RL.Lock()
	if c2, ok := Commit2[s]; ok {
		delete(Commit2, s)
		Commit2RL.Unlock()
		ctx.Result(http.StatusOK, *c2)
		return
	}
	Commit2RL.Unlock()
	ctx.Result(403, "未找到")
	return
}
func CompleteCommit2(ctx *Context) {
	var s server_c2.SectorIDCommit2
	if err := s.UnMarshal(ctx.request.Body); err != nil {
		log.Println("CompleteCommit2错误信息：", err.Error())
		ctx.Result(400, "CompleteCommit2错误信息！")
		return
	}

	file, _ := os.OpenFile(filepath.Join(server_c2.CSPATHC2, server_c2.SectorNumString(s.Sid)), os.O_CREATE|os.O_WRONLY, 0664)
	file.Write(s.Proof.Proof)
	defer file.Close()
	log.Println("完成信息：", s.Sid)
	Commit2RL.Lock()
	Commit2[s.Sid] = &s.Proof
	Commit2RL.Unlock()

	RunningRL.Lock()
	delete(Running, s.Sid)
	RunningRL.Unlock()
	os.Remove(filepath.Join(server_c2.CSPATHC1, server_c2.SectorNumString(s.Sid)))

	ctx.Result(http.StatusOK, "调用成功！")
	return
}

//func ConnectServer(ctx *Context) {
//	fmt.Println("hello worker")
//}

func ConnectServer(ctx *Context) {
	body, err := ioutil.ReadAll(ctx.request.Body)
	if err != nil {
		ctx.Result(400, err)
		return
	}
	param, err := server_c2.NewParam(body)
	if err != nil {
		ctx.Result(400, err)
		return
	}
	host := param["host"].(string)
	b := false
	if param["gpu"].(bool) {
		b = true
	}

	ip, _, err := net.SplitHostPort(strings.TrimSpace(ctx.request.RemoteAddr))

	if err == nil {
		iphost := ip + ":" + host
		WorkersRL.Lock()
		if client, ok := WorkersClient[iphost]; ok {
			client.GpuUse = b
			if err := client.CheckHeartbeat(); err == nil {
				WorkersRL.Unlock()
				ctx.Result(http.StatusOK, "心跳检测正常！")
				return
			}
		}
		WorkersRL.Unlock()

		conn, err := net.DialTimeout("tcp", iphost, time.Second*15)
		if err != nil {
			ctx.Result(500, err.Error())
			return
		}
		WorkersRL.Lock()
		WorkersClient[iphost] = NewWorkerInfo(iphost, jsonrpc.NewClient(conn))
		log.Println(iphost, "连接成功！")
		WorkersRL.Unlock()
		ctx.Result(http.StatusOK, "连接成功！")
		return
	}

	ctx.Result(403, err.Error())
}

func GpuCount(ctx *Context) {
	var count int
	WorkersRL.RLock()
	for _, v := range WorkersClient {
		if !v.GpuUse && !v.Disconnect {
			count++
		}
	}
	WorkersRL.RUnlock()
	ctx.Result(http.StatusOK, count)
	return
}
