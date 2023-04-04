package sealer

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-statestore"
	"sync"
)

type AlreadyTask int64
type LDBkey string

const (
	ALREADY_ISSUE_KEY = "already_issue_key"

	ALREADY_SUB_KEY = "already_sub"
)

var alMt sync.Mutex

func NewP1Count(store *statestore.StateStore) (AlreadyTask, error) {
	alMt.Lock()
	var a AlreadyTask
	if b, _ := store.Has(ALREADY_ISSUE_KEY); b {
		buf, err := store.GetByKey(ALREADY_ISSUE_KEY)
		if err != nil {
			return 0, err
		}
		if err = json.Unmarshal(buf, &a); err != nil {
			return 0, err
		}
	}
	fmt.Println("NewP1Count获取总量：", a)
	return a, nil
}
func (a *AlreadyTask) AddP1Count(store *statestore.StateStore) error {
	*a++
	fmt.Println("AddP1Count设置总量：", *a)
	return store.PutKey(ALREADY_ISSUE_KEY, a)
}

func (a *AlreadyTask) SubP1Count(store *statestore.StateStore) error {
	*a--
	fmt.Println("SubP1Count设置总量：", *a)
	return store.PutKey(ALREADY_ISSUE_KEY, a)
}
func (a *AlreadyTask) SetP1Count(store *statestore.StateStore, num int64) error {
	*a = AlreadyTask(num)
	fmt.Println("SetP1Count设置总量：", *a, num)
	return store.PutKey(ALREADY_ISSUE_KEY, a)
}
func (a *AlreadyTask) SubP1CountByNum(store *statestore.StateStore, num int64) error {
	*a -= AlreadyTask(num)
	fmt.Println("SubP1CountByNum设置总量：", *a, num)
	return store.PutKey(ALREADY_ISSUE_KEY, a)
}
func (a *AlreadyTask) AddP1CountByNum(store *statestore.StateStore, num int64) error {
	*a += AlreadyTask(num)
	fmt.Println("AddP1CountByNum设置总量：", *a, num)
	return store.PutKey(ALREADY_ISSUE_KEY, a)
}

func (a *AlreadyTask) FreeAlMt() {
	alMt.Unlock()
}
