package sealer

import (
	"context"
	"sync"
	"time"
	//yungojs
	"github.com/filecoin-project/go-statestore"

	"github.com/google/uuid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
//var InitWait = 3 * time.Second
var InitWait = 30 * time.Second //yungojs

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	// Ok is true if worker is acceptable for performing a task.
	// If any worker is preferred for a task, other workers won't be considered for that task.
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *WorkerHandle) (ok, preferred bool, err error)

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *WorkerHandle) (bool, error) // true if a is preferred over b
}

type Scheduler struct {
	assigner Assigner

	workersLk sync.RWMutex

	Workers map[storiface.WorkerID]*WorkerHandle

	schedule       chan *WorkerRequest
	windowRequests chan *SchedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	SchedQueue  *RequestQueue
	OpenWindows []*SchedWindowRequest

	workTracker *workTracker

	info      chan func(interface{})
	rmRequest chan *rmRequest

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing

	//yungojs
	sectorscalls *statestore.StateStore
	workcalls    *statestore.StateStore
	alreadycalls *statestore.StateStore
	workeripLk 	 sync.RWMutex
	workerip     map[storiface.WorkerID]string
	ipworkerLk 	 sync.RWMutex
	ipworker     map[string]storiface.WorkerID
	hostnameipLk sync.RWMutex
	hostnameip   map[string]string
}

type WorkerHandle struct {
	workerRpc Worker

	tasksCache  map[sealtasks.TaskType]struct{}
	tasksUpdate time.Time
	tasksLk     sync.Mutex

	Info storiface.WorkerInfo

	preparing *ActiveResources // use with WorkerHandle.lk
	active    *ActiveResources // use with WorkerHandle.lk

	lk sync.Mutex // can be taken inside sched.workersLk.RLock

	wndLk         sync.Mutex // can be taken inside sched.workersLk.RLock
	activeWindows []*SchedWindow

	Enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type SchedWindowRequest struct {
	Worker storiface.WorkerID

	Done chan *SchedWindow
}

type SchedWindow struct {
	Allocated ActiveResources
	Todo      []*WorkerRequest
}

type workerDisableReq struct {
	activeWindows []*SchedWindow
	wid           storiface.WorkerID
	done          func()
}

type WorkerRequest struct {
	Sector   storiface.SectorRef
	TaskType sealtasks.TaskType
	Priority int // larger values more important
	Sel      WorkerSelector
	SchedId  uuid.UUID

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	IndexHeap int
	ret       chan<- workerResponse
	Ctx       context.Context
}

type workerResponse struct {
	err error
}

type rmRequest struct {
	id  uuid.UUID
	res chan error
}
//yungojs
var Sectorscalls *statestore.StateStore
func newScheduler(assigner string,scalls *statestore.StateStore,tcalls *statestore.StateStore,acalls *statestore.StateStore) (*Scheduler, error) {
	var a Assigner
	switch assigner {
	case "", "utilization":
		a = NewLowestUtilizationAssigner()
	case "spread":
		a = NewSpreadAssigner()
	default:
		return nil, xerrors.Errorf("unknown assigner '%s'", assigner)
	}
	//yungojs
	Sectorscalls = scalls
	return &Scheduler{
		assigner: a,

		Workers: map[storiface.WorkerID]*WorkerHandle{},

		schedule:       make(chan *WorkerRequest),
		windowRequests: make(chan *SchedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		SchedQueue: &RequestQueue{},

		workTracker: &workTracker{
			done:     map[storiface.CallID]struct{}{},
			running:  map[storiface.CallID]trackedWork{},
			prepared: map[uuid.UUID]trackedWork{},
		},

		info:      make(chan func(interface{})),
		rmRequest: make(chan *rmRequest),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),

		//yungojs
		sectorscalls: scalls,
		workcalls:    tcalls,
		alreadycalls: acalls,
		workerip:     make(map[storiface.WorkerID]string),
		ipworker:     make(map[string]storiface.WorkerID),
		hostnameip:   make(map[string]string),
	}, nil
}

func (sh *Scheduler) Schedule(ctx context.Context, sector storiface.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &WorkerRequest{
		Sector:   sector,
		TaskType: taskType,
		Priority: getPriority(ctx),
		Sel:      sel,
		SchedId:  uuid.New(),

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		Ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *WorkerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.Ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

func (r *WorkerRequest) SealTask() sealtasks.SealTaskType {
	return sealtasks.SealTaskType{
		TaskType:            r.TaskType,
		RegisteredSealProof: r.Sector.ProofType,
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
	SchedId  uuid.UUID

	Ip  string //yungojs
	Wid string //yungojs
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *Scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool
	//yungojs
	rtime := time.Minute * 2
	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case rmreq := <-sh.rmRequest:
			sh.removeRequest(rmreq)
			doSched = true
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.SchedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case <-time.After(rtime): //yungojs
			log.Info("1进入任务队列。。。")
			doSched = true

		case req := <-sh.windowRequests:
			sh.OpenWindows = append(sh.OpenWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())
		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.SchedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.OpenWindows = append(sh.OpenWindows, req)
				case <-time.After(rtime): //yungojs
					log.Info("2进入任务队列。。。")
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.Todo {
						sh.SchedQueue.Push(request)
					}
				}

				openWindows := make([]*SchedWindowRequest, 0, len(sh.OpenWindows))
				for _, window := range sh.OpenWindows {
					if window.Worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.OpenWindows = openWindows

				sh.workersLk.Lock()
				sh.Workers[req.wid].Enabled = false
				sh.workersLk.Unlock()

				req.done()
			}
			//yungojs
			sh.trySched1()
		}

	}
}

func (sh *Scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.SchedQueue.Len(); sqi++ {
		task := (*sh.SchedQueue)[sqi]
		//yungojs
		var sect SectorTask
		b, _ := sh.sectorscalls.Has(task.Sector.ID.Number)
		if b {
			buf, err := sh.sectorscalls.GetByKey(task.Sector.ID.Number)
			if err != nil {
				log.Errorf("获取扇区记录错误 %s", err)
			}
			sect = NewSectorTask(buf)
			//sect.FreeSectMt()
		}
		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.Sector.ID,
			TaskType: task.TaskType,
			Priority: task.Priority,
			SchedId:  task.SchedId,

			Ip:       sect.Ip,           //yungojs
			Wid:      sect.Wid.String(), //yungojs
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.OpenWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.Worker).String())
	}

	return out
}

type Assigner interface {
	TrySched(sh *Scheduler)
}

func (sh *Scheduler) trySched() {
	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	sh.assigner.TrySched(sh)
}

func (sh *Scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.Workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *Scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *Scheduler) removeRequest(rmrequest *rmRequest) {

	if sh.SchedQueue.Len() < 0 {
		rmrequest.res <- xerrors.New("No requests in the scheduler")
		return
	}

	queue := sh.SchedQueue
	for i, r := range *queue {
		if r.SchedId == rmrequest.id {
			queue.Remove(i)
			rmrequest.res <- nil
			go r.respond(xerrors.Errorf("scheduling request removed"))
			return
		}
	}
	rmrequest.res <- xerrors.New("No request with provided details found")
}

func (sh *Scheduler) RemoveRequest(ctx context.Context, schedId uuid.UUID) error {
	ret := make(chan error, 1)

	select {
	case sh.rmRequest <- &rmRequest{
		id:  schedId,
		res: ret,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sh *Scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
