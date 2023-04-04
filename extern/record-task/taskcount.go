package record_task

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-statestore"
	"sync"
	"time"
)

//func (s *SectorTask)SectorTask(){
//
//}

type TaskCount struct {
	Wid     string
	APcount int
	P1count int
	Start   time.Time
}

func NewTaskCount(store *statestore.StateStore, wid string) TaskCount {
	taskMt.Lock()
	var t TaskCount
	if b, _ := store.Has(wid); b {
		buf, _ := store.GetByKey(wid)
		err := json.Unmarshal(buf, &t)
		if err != nil {
			fmt.Println("解析失败！:", err)
		}
		return t
	}
	t.Wid = wid
	return t
}
func ReadTaskCount(store *statestore.StateStore, wid string) TaskCount {
	//taskMt.RLock()
	//defer func() {
	//	taskMt.RUnlock()
	//}()
	var t TaskCount
	if b, _ := store.Has(wid); b {
		buf, _ := store.GetByKey(wid)
		err := json.Unmarshal(buf, &t)
		if err != nil {
			fmt.Println("解析失败！:", err)
		}
		return t
	}
	t.Wid = wid
	return t
}
func GetTaskList(store *statestore.StateStore) ([]TaskCount, error) {
	//taskMt.Lock()
	//defer func() {
	//	taskMt.Unlock()
	//}()
	var t []TaskCount
	err := store.ListKey(&t)
	return t, err
}
func (t *TaskCount) SetTaskCount(store *statestore.StateStore) error {
	taskMt.Lock()
	defer func() {
		taskMt.Unlock()
	}()
	//if b, _ := store.Has(t.Wid); !b {
	//	return errors.New("workerID不存在：" + t.Wid)
	//}
	fmt.Printf("%s:SetTaskCount设置数量：ap:%d,p1:%d\n", t.Wid, t.APcount, t.P1count)
	return store.PutKey(t.Wid, t)
}
func (t *TaskCount) DelTaskCount(store *statestore.StateStore) error {
	taskMt.Lock()
	defer func() {
		taskMt.Unlock()
	}()
	if b, _ := store.Has(t.Wid); !b {
		return errors.New("workerID不存在：" + t.Wid)
	}
	return store.Get(t.Wid).End()
}

var taskMt sync.RWMutex

//Ap任务数
func (t *TaskCount) AddAPcount(store *statestore.StateStore) error {
	t.APcount++
	fmt.Println(t.Wid, ":AddAPcount设置AP数量：", t.P1count, ",", t.Wid)
	return store.PutKey(t.Wid, t)
}
func (t *TaskCount) SubAPcount(store *statestore.StateStore) error {
	t.APcount--
	if t.APcount < t.P1count {
		t.APcount = t.P1count
	}
	fmt.Println(t.Wid, ":SubAPcount设置AP数量：", t.APcount)
	return store.PutKey(t.Wid, t)
}

//P1任务数
func (t *TaskCount) AddP1count(store *statestore.StateStore) error {
	t.P1count++
	fmt.Println(t.Wid, ":AddP1count设置P1数量：", t.P1count, ",", t.Wid)
	return store.PutKey(t.Wid, t)
}
func (t *TaskCount) SubP1count(store *statestore.StateStore) error {
	t.P1count--
	fmt.Println(t.Wid, ":SubP1count设置P1数量：", t.APcount, "P1数量：", t.P1count)
	return store.PutKey(t.Wid, t)
}
func (t *TaskCount) SubApAndP1count(store *statestore.StateStore) error {
	t.P1count--
	t.APcount = t.P1count
	fmt.Println(t.Wid, ":SubP1count设置P1数量：", t.APcount, "P1数量：", t.P1count)
	return store.PutKey(t.Wid, t)
}

func (t *TaskCount) FreeTaskMt() {
	taskMt.Unlock()
}

//func (c TaskCount) String() string {
//	return fmt.Sprintf(ALREADY_ISSUE_KEY)
//}
//
//var _ fmt.Stringer = &TaskCount{}

