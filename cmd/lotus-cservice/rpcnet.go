package main

import (
	"errors"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/server-c2"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var WorkersRL sync.RWMutex
var WorkersClient = make(map[string]*WorkerInfo)
var RunningRL sync.RWMutex
var Running = make(map[abi.SectorID]time.Time)
var Commit2RL sync.RWMutex
var Commit2 = make(map[abi.SectorID]*server_c2.ProofResult)

type WorkerInfo struct {
	GpuUse     bool
	Disconnect bool
	Host       string
	Client     *rpc.Client
	WorkerRL   sync.RWMutex
}

func NewWorkerInfo(host string, Client *rpc.Client) *WorkerInfo {
	return &WorkerInfo{
		GpuUse:     false,
		Disconnect: false,
		Host:       host,
		Client:     Client,
	}
}

func (w *WorkerInfo) RunCommit2(arggs server_c2.SealerParam) error {
	if w.GpuUse || w.Disconnect {
		return errors.New("设备正忙或者已掉线")
	}
	w.GpuUse = true
	defer func() {
		w.GpuUse = false
	}()
	var reply int
	if err := w.Client.Call("Sealer.RunCommit2", arggs, &reply); err != nil {
		return err
	}

	return nil
}
func (s *WorkerInfo) GetCommit2(args abi.SectorID, reply *storiface.Proof) error {
	_, err := os.Stat(filepath.Join(server_c2.PATHC2, server_c2.SectorNumString(args)))
	if os.IsNotExist(err) {
		return err
	}
	proof, err := ioutil.ReadFile(filepath.Join(server_c2.PATHC2, server_c2.SectorNumString(args)))
	if err != nil {
		log.Println("读取C2结果失败！", args.Number, server_c2.PATHC2)
		return err
	}
	*reply = proof

	fmt.Printf("proof: %x\n", proof)

	return err
}

//心跳检测
func (w *WorkerInfo) CheckHeartbeat() error {
	var b int
	if err := w.Client.Call("Sealer.Heartbeat", nil, &b); err != nil {
		log.Println("心跳检测错误：", err.Error())
		//conn, err := net.DialTimeout("tcp", w.Host, time.Second*15)
		//if err != nil {
		//	return err
		//}
		w.WorkerRL.Lock()
		w.Disconnect = false
		w.WorkerRL.Unlock()
		//w.Client = jsonrpc.NewClient(conn)
		return err
	}
	return nil
}
