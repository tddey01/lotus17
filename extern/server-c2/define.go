package server_c2

import (
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
)

var PATHC2 = "./proof/"
var PATHC1 = "./phase/"
var CSPATHC2 = "./proofcs/"
var CSPATHC1 = "./phasecs/"
func SectorNumString(id abi.SectorID)string{
	return fmt.Sprintf("s-t0%s-%d",id.Miner.String(),id.Number)
}
