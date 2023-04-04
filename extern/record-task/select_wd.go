package record_task

import (
	"sync"
	"time"
)

type SelectWD struct {
	SelectLR sync.RWMutex
	WdTask map[string]*TaskCount
}

var Wselect = SelectWD{WdTask: make(map[string]*TaskCount)}

func (s *SelectWD) Have(wid string) bool {

	if _, ok := s.WdTask[wid]; !ok {
		return false
	}
	return true
}
func (s *SelectWD) Has(wid string) bool {
	if _, ok := s.WdTask[wid]; !ok {
		return false
	}
	return true
}
func (s *SelectWD) GetP1(wid string) int {

	if !s.Has(wid) {
		return 0
	}
	return s.WdTask[wid].P1count
}
func (s *SelectWD) SubP1(wid string) {

	sw := new(TaskCount)
	if s.Has(wid) {
		sw = s.WdTask[wid]
	} else {
		s.WdTask[wid] = sw
	}
	if sw.P1count>0{
		sw.P1count--
	}
	return
}
func (s *SelectWD) AddP1Start(wid string) {

	sw := new(TaskCount)
	if s.Has(wid) {
		sw = s.WdTask[wid]
	} else {
		s.WdTask[wid] = sw
	}
	sw.Start = time.Now()
	sw.P1count++
	return
}
func (s *SelectWD) AddP1(wid string) {

	sw := new(TaskCount)
	if s.Has(wid) {
		sw = s.WdTask[wid]
	} else {
		s.WdTask[wid] = sw
	}
	sw.P1count++
	return
}
