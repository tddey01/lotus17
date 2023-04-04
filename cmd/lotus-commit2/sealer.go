package main

import (
	"encoding/json"
	"fmt"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/lotus/extern/server-c2"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)
type Sealer struct {
}

func (s *Sealer) RunCommit2(args server_c2.SealerParam, reply *int)error {
	//var err error
	go func() {
		CInfo.InfoRL.Lock()
		CInfo.Gpu = true
		CInfo.InfoRL.Unlock()
		fmt.Printf("----\nstart proof computation\n")
		start := time.Now()
		//保存正在做的参数
		args.Status = 1
		file1,_ := os.OpenFile(filepath.Join(server_c2.PATHC1, server_c2.SectorNumString(args.Sector.ID)),os.O_CREATE|os.O_WRONLY,0664)
		//移除正在做的任务
		defer os.Remove(filepath.Join(server_c2.PATHC1, server_c2.SectorNumString(args.Sector.ID)))
		buf,_ := json.Marshal(args)
		file1.Write(buf)
		file1.Close()
		var Err string
		proof, err := ffi.SealCommitPhase2(args.Phase1Out, args.Sector.ID.Number, args.Sector.ID.Miner)
		if err != nil {
			Err = err.Error()
			log.Println("commit2 失败：", err.Error())
			//return
		}
		CInfo.InfoRL.Lock()
		CInfo.Gpu = false
		CInfo.InfoRL.Unlock()

		fmt.Printf("proof: %x\n", proof)

		dur := time.Now().Sub(start)

		fmt.Printf("seal: commit phase 2: %s\n", dur)
		sc := server_c2.SectorIDCommit2{
			args.Sector.ID,
			server_c2.ProofResult{proof,Err},
		}
		var rp server_c2.Respones
		file,_ := os.OpenFile(filepath.Join(server_c2.PATHC2, server_c2.SectorNumString(args.Sector.ID)),os.O_CREATE|os.O_WRONLY,0664)
		defer file.Close()
		_,err = file.Write(proof)

		for {
			if err := server_c2.RequestDo(CInfo.Url,"/completecommit2", &sc, &rp,time.Minute); err != nil||rp.Code!=http.StatusOK {
				log.Println(err,rp.Data)
			}else{
				_ = os.Remove(filepath.Join(server_c2.PATHC2, server_c2.SectorNumString(args.Sector.ID)))
				return
			}
			time.Sleep(time.Second)
		}

	}()
	return nil
}

func (s *Sealer) Heartbeat(args int, reply *int)error {
	//info,err := os.Stat(PATHC2)
	//if err!=nil{
	//	log.Println(err.Error())
	//}
	//info.ModTime()

	log.Println("心跳开始")
	return nil
}


