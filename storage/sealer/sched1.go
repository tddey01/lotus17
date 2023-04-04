package sealer

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	record "github.com/filecoin-project/lotus/extern/record-task"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"sort"
	"strings"
	"sync"
)

func (sh *Scheduler) trySched1() {
	/*
		This assigns tasks to Workers based on:
		- Task priority (achieved by handling sh.SchedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified Worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the SchedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through SchedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to Workers

	*/
	log.Debugf("中创try0Sched ")
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()
	sh.workeripLk.RLock()
	defer sh.workeripLk.RUnlock()
	sh.ipworkerLk.RLock()
	defer sh.ipworkerLk.RUnlock()

	windowsLen := len(sh.OpenWindows)
	queuneLen := sh.SchedQueue.Len()

	log.Debugf("中创trySched: SCHED %d queued; %d open windows %v", queuneLen, windowsLen, record.FINsuspend)

	if windowsLen == 0 && queuneLen == 0 {
		// nothing to schedule on
		return
	}
	windows := make([]SchedWindow, windowsLen)
	for i := range windows {
		windows[i].Allocated = *NewActiveResources()
	}
	acceptableWindows := make([][]int, queuneLen)

	taskmap := make(map[string]int)
	conf := GetPledgeConfig()
	rmQueue := make([]int, 0, queuneLen)
	//遍历查询被绑定worker任务
	apHave := make(map[storiface.WorkerID]int)
	rmCommit1 := make(map[abi.SectorNumber]struct{})

	for i := 0; i < queuneLen; i++ {
		task := (*sh.SchedQueue)[i]
		if task.TaskType != sealtasks.TTPreCommit1 && task.TaskType != sealtasks.TTAddPiece {
			continue
		}
		var sect SectorTask
		b, _ := sh.sectorscalls.Has(task.Sector.ID.Number)
		if b {
			buf, err := sh.sectorscalls.GetByKey(task.Sector.ID.Number)
			if err != nil {
				log.Errorf("获取扇区记录错误 %s", err)
			}
			sect = NewSectorTask(buf)
			//IP为空，小概率事件
			switch task.TaskType {
			case sealtasks.TTPreCommit1:
				if sect.Wid.String() != "" && sect.Ip != "" {
					apHave[sect.Wid]++
				}
			case sealtasks.TTCommit1:
				if sect.Prove {
					log.Info("Prove:", sect)
					if err := MinerApi.SectorsUpdate(context.TODO(), sect.Sector, "SubmitCommitAggregate"); err != nil {
						log.Error("修改错误：", err.Error())
						continue
					}
					//rmQueue = append(rmQueue, i)
					rmCommit1[task.Sector.ID.Number] = struct{}{}
				}
			}

			//sect.FreeSectMt()
		}
		taskmap[task.TaskType.Short()+"---"+sect.Ip+"---"+sect.Wid.String()]++
	}
	for k, v := range taskmap {
		fmt.Println("任务列表：", k, v)
	}
	// Step 1
	var wg sync.WaitGroup
	wg.Add(queuneLen)
	record.RemoveRL.RLock()

	for i := 0; i < queuneLen; i++ {
		var ok bool
		if _, ok = record.RemoveSectors[(*sh.SchedQueue)[i].Sector.ID.Number]; ok {
			rmQueue = append(rmQueue, i)
			wg.Done()
			continue
		}
		if _, ok = rmCommit1[(*sh.SchedQueue)[i].Sector.ID.Number]; ok {
			rmQueue = append(rmQueue, i)
			wg.Done()
			continue
		}

		go func(sqi int) {
			defer wg.Done()

			task := (*sh.SchedQueue)[sqi]
			task.IndexHeap = sqi
			if (sealtasks.TTFinalize == task.TaskType || sealtasks.TTFetch == task.TaskType) && record.FINsuspend {
				return
			}

			var sect SectorTask

			if task.TaskType != sealtasks.TTAddPiece {
				sect = GetSectorTask(sh.sectorscalls, task.Sector.ID.Number)
				if sect.Ip == "" {
					log.Info("未获取扇区信息1:", task.Sector.ID.Number, ",", task.TaskType)
					return
				}
			}
			if sect.Ip != "" && task.TaskType != sealtasks.TTCommit2 && task.TaskType != sealtasks.TTAddPiece {
				//log.Info(sqi, ":测1试：扇区ID ", task.Sector.ID.Number, ",任务类型：", task.TaskType)
				first := true

				for wnd, windowRequest := range sh.OpenWindows {
					//log.Info(wnd,"扇区：",task.Sector.ID.Number,",任务worker检测：",windowRequest.Worker.String(),"，历史worker：",sect.Wid.String(),"，判断：",sh.CheckWindow(windowRequest,task,selewind))
					host := sh.workerip[windowRequest.Worker]
					ips := strings.Split(host, ":")
					if len(ips) == 0 {
						log.Error("workerid:%s,ip不存在！%s", windowRequest.Worker.String(), sect.Ip)
						continue
					}
					//if task.TaskType == sealtasks.TTPreCommit1 {
					//	log.Info("ips[0]:", ips[0], ",Ip:", sect.Ip, ",", sh.CheckWindow1(windowRequest, task,conf))
					//}

					if ips[0] == sect.Ip {
						if sealtasks.TTFinalize == task.TaskType || sealtasks.TTFetch == task.TaskType {
							log.Info("选择同IP worker进行任务：", sect.Sector, ",", sect.Ip, ",", task.TaskType)
							acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
							if first {
								first = false
								continue
							}
							break
							//}
						}

						worker, ok := sh.Workers[windowRequest.Worker]
						if !ok {
							log.Errorf("Worker referenced by windowRequest not found (Worker: %s)", windowRequest.Worker)
							// TODO: How to move forward here?
							continue
						}

						if !worker.Enabled {
							log.Debugw("skipping disabled Worker", "Worker", windowRequest.Worker)
							if windowRequest.Worker.String() == conf.Debugwid {
								log.Errorf("中创106 skipping disabled Worker", "Worker", windowRequest.Worker)
							}
							continue
						}

						rpcCtx, cancel := context.WithTimeout(task.Ctx, SelectorTimeout)
						ok, _, err := task.Sel.Ok(rpcCtx, task.TaskType, task.Sector.ProofType, worker)
						cancel()
						if err != nil {
							log.Errorf("trySched(1) req.Sel.Ok error: %+v", err)
							continue
						}

						if !ok {
							if windowRequest.Worker.String() == conf.Debugwid {
								log.Error("中创106 req.sel.Ok error: ", ok, ",", task.Sector.ID, ",", task.TaskType)
							}
							continue
						}

						record.Wselect.SelectLR.Lock()
						if sh.checkedTasktype1(windowRequest.Worker, task, conf) {
							acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
							if first {
								//record.Wselect.SelectLR.Lock()
								log.Info("任务类型1：", task.TaskType, ",扇区id：", task.Sector.ID.Number, ",", sh.workerip[windowRequest.Worker])
								switch task.TaskType {
								case sealtasks.TTAddPiece:
									record.Wselect.AddP1(windowRequest.Worker.String())
								case sealtasks.TTPreCommit1:
									//record.DelayControl.AddP1(windowRequest.Worker.String())
									record.Wselect.AddP1Start(windowRequest.Worker.String())
									//case sealtasks.TTPreCommit2:
									//	record.Wselect.AddP2(windowRequest.Worker.String())
								}
								first = false
								//log.Info("扫描",sqi,"任务结束1：",task.TaskType)
								record.Wselect.SelectLR.Unlock()
								continue
							}
							//log.Info("扫描",sqi,"任务结束2：",task.TaskType)
							record.Wselect.SelectLR.Unlock()
							break
						}
						//log.Info("扫描",sqi,"任务结束3：",task.TaskType)
						record.Wselect.SelectLR.Unlock()
					}
				}
			} else {
				//log.Info(sqi,":测试：扇区ID ",task.Sector.ID.Number,",任务类型：",task.TaskType)
				first := true
				for wnd, windowRequest := range sh.OpenWindows { //遍历worker
					worker, ok := sh.Workers[windowRequest.Worker]
					//已有任务不接新任务
					if apHave[windowRequest.Worker] >= 14 && task.TaskType != sealtasks.TTCommit2 {
						//if windowRequest.Worker.String() == "5337cef1-a65d-4e19-b60b-8eedba3ba659" {
						//	log.Info("aphvae：", task.TaskType, ",扇区id：", task.Sector.ID.Number,",workerID:",windowRequest.Worker.String() )
						//}
						continue
					}

					if !ok {
						log.Errorf("Worker referenced by windowRequest not found (Worker: %s)", windowRequest.Worker)
						// TODO: How to move forward here?
						continue
					}
					if !worker.Enabled {
						log.Debugw("skipping disabled Worker", "Worker", windowRequest.Worker)
						continue
					}
					// TODO: allow bigger windows
					rpcCtx, cancel := context.WithTimeout(task.Ctx, SelectorTimeout)
					ok, _, err := task.Sel.Ok(rpcCtx, task.TaskType, task.Sector.ProofType, worker)
					cancel()
					if err != nil {
						log.Errorf("trySched(1) req.Sel.Ok error: %+v", err)
						continue
					}

					if !ok {
						if windowRequest.Worker.String() == conf.Debugwid {
							log.Error("中创106 req.sel.Ok error: ", ok, ",", task.Sector.ID, ",", task.TaskType)
						}
						continue
					}
					record.Wselect.SelectLR.Lock()
					if sh.checkedTasktype1(windowRequest.Worker, task, conf) {
						acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
						if first {
							log.Info("任务类型：", task.TaskType, ",扇区id：", task.Sector.ID.Number, ",", sh.workerip[windowRequest.Worker])
							switch task.TaskType {
							case sealtasks.TTAddPiece:
								record.Wselect.AddP1(windowRequest.Worker.String())
							case sealtasks.TTPreCommit1:
								//record.DelayControl.AddP1(windowRequest.Worker.String())
								record.Wselect.AddP1Start(windowRequest.Worker.String())
								//case sealtasks.TTPreCommit2:
								//	record.Wselect.AddP2(windowRequest.Worker.String())
								//case sealtasks.TTCommit2:
								//	record.Wselect.AddC2(windowRequest.Worker.String())
							}
							first = false
							//log.Info("扫描",sqi,"任务结束11：",task.TaskType)
							record.Wselect.SelectLR.Unlock()
							continue
						}
						if sealtasks.TTCommit2 != task.TaskType {
							//log.Info("扫描",sqi,"任务结束22：",task.TaskType)
							record.Wselect.SelectLR.Unlock()
							break
						}
					}
					//log.Info("扫描",sqi,"任务结束33：",task.TaskType)
					record.Wselect.SelectLR.Unlock()
					//log.Warnf("Worker:%+v 任务数过多 %s ",windowRequest.Worker,task.TaskType)
				}
			}
		}(i)
	}
	record.RemoveRL.RUnlock()
	wg.Wait()
	log.Debugf("中创try1Sched ")

	//log.Debugf("中创trySched SCHED windows: %+v", windows)
	//log.Debugf("中创trySched SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	for sqi := 0; sqi < queuneLen; sqi++ {
		task := (*sh.SchedQueue)[sqi]
		needRes := storiface.ResourceTable[task.TaskType][task.Sector.ProofType]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.IndexHeap] { //379
			wid := sh.OpenWindows[wnd].Worker
			wr := sh.Workers[wid].Info

			windows[wnd].Allocated.Add(task.SealTask(), wr.Resources, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].Todo = append(windows[selectedWindow].Todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}
	sort.Ints(rmQueue)

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.SchedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window2 := range windows {
		if len(window2.Todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window1 := window2 // copy
		select {
		case sh.OpenWindows[wnd].Done <- &window1:
			for _, v := range window1.Todo {
				if v == nil {
					continue
				}
				log.Info("任务往chan发送 扇区id:", v.Sector.ID, ",任务类型：", v.TaskType)
			}
		default:
			log.Error("expected sh.OpenWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.OpenWindows array, removing scheduled windows
	newOpenWindows := make([]*SchedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.OpenWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.OpenWindows = newOpenWindows
}

func (sh *Scheduler) GetTodoCountById(wid storiface.WorkerID, task sealtasks.TaskType) int {
	count := 0
	for _, v := range sh.Workers[wid].activeWindows {
		for _, val := range v.Todo {
			switch val.TaskType {
			case sealtasks.TTAddPiece:
				if task == sealtasks.TTAddPiece || task == sealtasks.TTPreCommit1 {
					count++
				}
			case sealtasks.TTPreCommit1:
				if task == sealtasks.TTAddPiece || task == sealtasks.TTPreCommit1 {
					count++
				}
			case sealtasks.TTPreCommit2:
				if task == sealtasks.TTPreCommit2 {
					count++
				}
			case sealtasks.TTCommit2:
				if task == sealtasks.TTCommit2 {
					count++
				}
			}
		}

	}
	return count
}

func (sh *Scheduler) checkedTasktype1(wid storiface.WorkerID, task *WorkerRequest, conf PledgeConfig) bool {
	if task.TaskType == sealtasks.TTCommit2 {
		return true
	}
	mp := make(map[storiface.WorkerID]WConfig)
	for _, v := range conf.WorkerConfigs {
		mp[sh.ipworker[v.Ip]] = v
	}
	mpcount := 0
	selecount := 0

	tc := record.ReadTaskCount(sh.workcalls, wid.String())
	//defer tc.FreeTaskMt()
	switch task.TaskType {
	case sealtasks.TTAddPiece:
		if v, ok := mp[wid]; ok {
			mpcount = v.P1count
			if mpcount > conf.MaxP1count {
				mpcount = conf.MaxP1count
			}
		}
		if record.Wselect.Have(wid.String()) {
			selecount = record.Wselect.GetP1(wid.String())
			if selecount > 0 {
				return false
			}
		}

		if mpcount <= tc.APcount+selecount+sh.GetTodoCountById(wid, task.TaskType) {
			sect := GetSectorTask(sh.sectorscalls, task.Sector.ID.Number)
			if sect.Wid.String() == "00000000-0000-0000-0000-000000000000" {
				if wid.String() == conf.Debugwid {
					log.Info(mp[wid], "----", wid, "检查1：任务数过多：", mpcount, ",", tc.APcount, ",", selecount, ",", sh.GetTodoCountById(wid, task.TaskType))
				}
				return false
			}
		}
		if mpcount <= tc.P1count+selecount+sh.GetTodoCountById(wid, task.TaskType) {
			if wid.String() == conf.Debugwid {
				log.Info(mp[wid], "----", wid, "检查2：任务数过多：", mpcount, ",", tc.P1count, ",", selecount, ",", sh.GetTodoCountById(wid, task.TaskType))
			}
			return false
		}
	case sealtasks.TTPreCommit1:
		if v, ok := mp[wid]; ok {
			mpcount = v.P1count
			if mpcount > conf.MaxP1count {
				mpcount = conf.MaxP1count
			}
		}
		if record.Wselect.Have(wid.String()) {
			selecount = record.Wselect.GetP1(wid.String())
			if selecount > 0 {
				return false
			}
		}
		if mpcount <= tc.P1count+selecount+sh.GetTodoCountById(wid, task.TaskType) {
			if wid.String() == conf.Debugwid {
				log.Info(mp[wid], "----", wid, "中创106：任务数过多：", mpcount, ",", tc.P1count, ",", selecount, ",", sh.GetTodoCountById(wid, task.TaskType))
			}
			//log.Info(mp[wid], "----", wid, "检查3：任务数过多：", mpcount, ",", tc.P1count, ",", selecount)
			return false
		}
	}
	return true
}
