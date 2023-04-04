package currency_api

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/extern/record-task"
)

type StorageCalls interface {
	//yungojs
	SchedAlreadyIssueInfo(ctx context.Context) (int64, error)                          //perm:admin
	SchedSetAlreadyIssue(ctx context.Context, num int64) error                         //perm:admin
	SchedAddAlreadyIssue(ctx context.Context, num int64) error                         //perm:admin
	SchedSubAlreadyIssue(ctx context.Context, num int64) error                         //perm:admin
	WorkerSetTaskCount(ctx context.Context, tc record_task.TaskCount) error            //perm:admin
	WorkerGetTaskCount(ctx context.Context, wid string) (record_task.TaskCount, error) //perm:admin
	ActorAddress(context.Context) (address.Address, error)                             //perm:read
	PledgeSector(context.Context, uint64) (abi.SectorID, error)                        //perm:write
	SectorsUpdate(context.Context, abi.SectorNumber, api.SectorState) error            //perm:admin
}
