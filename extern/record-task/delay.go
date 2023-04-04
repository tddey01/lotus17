package record_task

import (
	"sync"
)

//var DelayControl = DelayP1{deP1: make(map[string]*Delay)}
var DelayMT sync.RWMutex
type DelayP1 struct {
	deP1 map[string]*Delay
}

func (s *DelayP1) Has(wid string) bool {
	if _, ok := s.deP1[wid]; !ok {
		return false
	}
	return true
}

type Delay struct {
	//delayMT sync.Mutex
	count   int
}

func (d *DelayP1) AddP1(wid string) {
	dp := new(Delay)
	if d.Has(wid) {
		dp = d.deP1[wid]
	} else {
		d.deP1[wid] = dp
	}
	DelayMT.Lock()
	defer DelayMT.Unlock()
	//dp.delayMT.Lock()
	//defer func() {
	//	dp.delayMT.Unlock()
	//}()
	dp.count++

	return
}
func (d *DelayP1) GetP1(wid string) int {
	if !d.Has(wid) {
		return 0
	}
	DelayMT.RLock()
	defer DelayMT.RUnlock()
	return d.deP1[wid].count
}

func (d *DelayP1) Reset() {
	for _, v := range d.deP1 {
		//v.delayMT.Lock()
		v.count = 0
		//v.delayMT.Unlock()
	}
	DelayMT.Lock()
	defer DelayMT.Unlock()
	return
}
var FINsuspendL sync.Mutex
var FINsuspend bool

