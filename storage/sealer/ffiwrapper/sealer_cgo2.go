package ffiwrapper

import (
	"context"
	"errors"
	"fmt"
	ffi "github.com/filecoin-project/filecoin-ffi"
	server_c2 "github.com/filecoin-project/lotus/extern/server-c2"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"net/http"
	"os"
	"time"
)

var URLC2 []*server_c2.C2Server

var CHANC2 = make(chan SectorCommit)
var C2Change = make(chan bool)
type SectorCommit struct {
	sealParam server_c2.SealerParam
	endc2 chan server_c2.ProofResult
}

func (sb *Sealer) SealCommit2(ctx context.Context, sector storiface.SectorRef, phase1Out storiface.Commit1Out) (storiface.Proof, error) {
	if os.Getenv("C2_MANAGE")=="true"{
		end := make(chan server_c2.ProofResult)

		CHANC2<-SectorCommit{server_c2.SealerParam{sector,phase1Out,0},end}
		select {
		case pf := <-end:
			log.Info("接收到 ",sector.ID," len(pf.Proof):",len(pf.Proof)," error：",pf.Err)
			if pf.Err!=""{
				return pf.Proof,errors.New(pf.Err)
			}
			return pf.Proof,nil
		}
	}
	return ffi.SealCommitPhase2(phase1Out, sector.ID.Number, sector.ID.Miner)
}

func TrySched(){
	var queueC2 []SectorCommit
	rtime := time.Second * 10
	for {
		select {
		case sc := <-CHANC2:
			queueC2 = append(queueC2, sc)
		case <-time.After(rtime):
		case <-C2Change:
		}
		//获取远程任务是否有闲置GPU
		for _,c2svr := range URLC2 {
			//更新数量
			if err := c2svr.UpdateGpuCount();err!=nil{
				log.Error("更新GPU数量错误：",err.Error())
				break
			}
		}
		runbool := false
		var pre []SectorCommit

		for i,task := range queueC2 {
			fmt.Print("任务列表：", task.sealParam.Sector.ID)
			fmt.Println()
			var c2svr *server_c2.C2Server
			for _,v := range URLC2 {
				if v.GpuCount > 0{
					c2svr = v
					break
				}
			}
			if c2svr == nil{
				log.Info("暂无可用GPU。。。")
				break
			}
			c2svr.GpuCount--
			err := SealCommit2(task,c2svr.Url)
			if err != nil{
				log.Error("发送C2任务报错：",err.Error())
				break
			}
			runbool = true
			if len(queueC2)>1{
				pre = queueC2[i+1:]
			}
		}
		if runbool{
			queueC2 = pre
		}
	}
}
//发任务过去
func SealCommit2(task SectorCommit,url string)error {
	//发送任务
	res ,err := task.sealParam.SealCommit2(url)
	if err != nil {
		return err
	}
	log.Info("开始扇区：",task.sealParam.Sector.ID)
	go func(sc SectorCommit,re server_c2.Respones) {
		//获取结果
		if re.Code != http.StatusCreated{
			time.Sleep(time.Minute*5)
		}
		for {
			var pr server_c2.ProofResult
			if err := pr.GetCommit2(sc.sealParam.Sector.ID,url);err!=nil&&pr.Err==""{
				time.Sleep(time.Second*10)
				//log.Info("开始获取：",sc.sealParam.Sector.ID,err)
				continue
			}
			log.Info(sc.sealParam.Sector.ID,"获取结果：",len(pr.Proof),pr.Err)
			//返回结果
			sc.endc2<-pr
			C2Change <- true
			break
		}
	}(task,res)

	return nil
}

