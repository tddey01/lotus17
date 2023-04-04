package sealer

import (
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type WorkerIdKey struct {
	workerid string
}

func (w WorkerIdKey) String() string {
	return fmt.Sprintf(w.workerid)
}

type SectorTask struct {
	Sector    abi.SectorNumber
	Ip        string
	Wid       storiface.WorkerID
	Prove     bool
}
func NewSectorTask(value []byte) SectorTask {
	var res SectorTask
	_ = json.Unmarshal(value, &res)
	return res
}
func GetSectorTask(sectorscalls *statestore.StateStore, Number abi.SectorNumber) SectorTask {
	var res SectorTask
	//sectMt.Lock()
	if b, _ := sectorscalls.Has(Number); b {
		buf, _ := sectorscalls.GetByKey(Number)
		_ = json.Unmarshal(buf, &res)
	}
	return res
}
