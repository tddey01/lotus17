package server_c2

import (
	"github.com/filecoin-project/go-state-types/abi"
	"regexp"
	"strconv"
	"strings"
)

func CheckSectorNum(buf string)bool{
	reg := regexp.MustCompile(`s-t0\d*-\d*`)
	result := reg.FindString(buf)
	if result == ""||result!=buf{
		return false
	}
	return true
}

func StringToSectorID(sn string)abi.SectorID{
	sn = strings.ReplaceAll(sn,"s-t0","")
	strs := strings.Split(sn,"-")
	if len(strs)<2{
		return abi.SectorID{}
	}
	m,_ := strconv.ParseUint(strs[0],10,64)
	n,_ := strconv.ParseUint(strs[0],10,64)
	return abi.SectorID{abi.ActorID(m),abi.SectorNumber(n)}
}
