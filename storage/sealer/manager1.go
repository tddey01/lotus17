package sealer

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/filecoin-project/lotus/chain/types"
	currency "github.com/filecoin-project/lotus/extern/currency-api"
	record "github.com/filecoin-project/lotus/extern/record-task"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

type PledgeConfig struct {
	WorkerConfigs  []WConfig
	MaxP1count     int
	WorkerBlannce  float64
	DelayMinute    int
	EveryCount     int
	FinSleepMinute int
	DBname         string
	DBhost         string
	DBusername     string
	DBpassword     string
	DBtable        string
	Debugwid       string
}

func (conf *PledgeConfig) SaveConfigFile() error {
	b, err := json.Marshal(*conf)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile)); err != nil {
		return err
	}
	f, _ := os.OpenFile(filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile), os.O_CREATE|os.O_WRONLY, 0644)
	_, err = f.Write(out.Bytes())
	defer f.Close()
	return err
}
func GetPledgeConfig() PledgeConfig {
	var conf PledgeConfig
	//log.Info("读取配置文件：", filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile))
	file, err := ioutil.ReadFile(filepath.Join(os.Getenv("LOTUS_MINER_PATH"), PledgeFile))
	if err != nil {
		return PledgeConfig{}
	}
	if err = json.Unmarshal(file, &conf); err != nil {
		return PledgeConfig{}
	}
	//log.Info("配置信息:", conf)
	return conf
}

type WConfig struct {
	Ip      string
	P1count int
}

const PledgeFile = "pledgeconfig.json"

var MinerApi currency.StorageCalls
var FullApi currency.NodeCalls

func (m *Manager) AutoPledge(ctx context.Context) {
	//lastTime := time.Now()
	for {
		log.Info("开始分发任务")
		time.Sleep(time.Minute * 1)
		log.Info("判断延迟时间")
		conf := GetPledgeConfig()
		record.Wselect.SelectLR.Lock()
		for _, v := range record.Wselect.WdTask {
			if v.Start != (time.Time{}) {
				if v.Start.Add(time.Duration(conf.DelayMinute/2) * time.Minute).Before(time.Now()) {
					if v.P1count > 0 {
						v.P1count = 0
					}
					v.Start = time.Now()
					log.Info("减少延迟数量。。。", v.Wid, v.P1count)
				}
			} else {
				v.P1count = 0
			}
		}
		record.Wselect.SelectLR.Unlock()
		if MinerApi == nil {
			continue
		}
		//record.Wselect.SubAll()
		maddr, err := MinerApi.ActorAddress(ctx)
		if err != nil {
			log.Error("中创ActorAddress", err)
			continue
		}
		mi, err := FullApi.StateMinerInfo(ctx, maddr, types.EmptyTSK)
		if err != nil {
			log.Error("中创StateMinerInfo", err)
			continue
		}
		wb, err := FullApi.WalletBalance(ctx, mi.Worker)
		if err != nil {
			log.Error("中创WalletBalance", err)
			continue
		}
		//log.Info("配置信息：",conf)
		//更新延迟
		//if time.Since(lastTime) >= time.Duration(conf.DelayMinute)*time.Minute {
		//	log.Info("重置延迟数量。。。", conf.DelayMinute)
		//	lastTime = time.Now()
		//	record.DelayControl.Reset()
		//}

		//判断worker blannce是否充足
		if NanoOrAttoToFIL(wb.String(), AttoFIL) < conf.WorkerBlannce {
			log.Warn("未达到设置金额！")
			continue
		}
		//获取IP任务数IpTaskCount()
		//iptask := m.IpTaskCount()
		//taskcount := m.AllowedTasksCount(conf) - m.sched.schedQueue.P1AndApCount()
		//for i:=0;i<taskcount;i++{
		//
		//
		//	id,err := MinerApi.PledgeSector(ctx)
		//	if err != nil {
		//		log.Error(err)
		//		return
		//	}
		//	log.Info("Created CC sector: ", id.Number)
		//	time.Sleep(time.Millisecond*100)
		//}
		allowed := 0
		//offLine := 0
		for _, v := range m.sched.Workers {
			//if _, err := v.workerRpc.Session(ctx); err != nil {
			//	tc := record.ReadTaskCount(m.sched.workcalls, v.info.Wid.String())
			//	offLine += tc.P1count
			//	continue
			//}

			//workerHostAllowCount := m.workerHostDiskAllowCount(v)
			//log.Infof("worker 主机允许的任务数, workerID:%s, 允许任务数量：%v", v.info.Wid.String(), workerHostAllowCount)
			//配置任务数
			for _, cf := range conf.WorkerConfigs {
				if cf.Ip == v.Info.Ip {
					num := cf.P1count
					if cf.P1count > conf.MaxP1count {
						num = conf.MaxP1count
					}

					// 取配置和可运行的最小值
					//if num > int(workerHostAllowCount) {
					//	num = int(workerHostAllowCount)
					//}
					allowed += num
				}
			}
		}
		log.Info("减掉已经发出去的任务数")
		//减掉已经发布的
		aly, err := NewP1Count(m.sched.alreadycalls)
		if err != nil {
			log.Error("自动发任务异常：", err)
			continue
		}
		//allowed = allowed - int(aly) + offLine
		allowed -= int(aly)
		log.Info("发任务开始：")
		for i := 0; i < allowed; i++ {
			//任务控制
			if err := aly.AddP1Count(m.sched.alreadycalls); err != nil {
				log.Error("发任务异常！", err)
				continue
			}

			id, err := MinerApi.PledgeSector(ctx, 0)
			if err != nil {
				log.Error("中创PledgeSector", err.Error())
				continue
			}
			log.Info("Created CC sector: ", id.Number)
			time.Sleep(time.Millisecond * 100)
		}
		aly.FreeAlMt()

		log.Info("发任务：", allowed, "，总共有：", aly)
	}
}
