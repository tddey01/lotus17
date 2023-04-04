package record_task

import (
	"github.com/filecoin-project/go-state-types/abi"
	"sync"
)

var RemoveSectors = make(map[abi.SectorNumber]struct{})
var RemoveRL sync.RWMutex
