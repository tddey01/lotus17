package server_c2

import (
	"bytes"
	"encoding/json"
	"github.com/filecoin-project/go-state-types/abi"
	"io"
	"io/ioutil"
	"log"
)

//var WorkerRun = make(map[string]bool)

type SectorIDCommit2 struct {
	Sid   abi.SectorID
	Proof ProofResult
}

func (s *SectorIDCommit2) Marshal() io.Reader {
	buf, err := json.Marshal(*s)
	if err != nil {
		log.Println("序列化失败：", err.Error())
		return nil
	}
	return bytes.NewReader(buf)
}
func (s *SectorIDCommit2) UnMarshal(r io.Reader) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	*s = SectorIDCommit2{}
	if err := json.Unmarshal(body, s); err != nil {
		log.Println("解析错误：", err.Error())
		return err
	}
	return nil
}


type Commit2In struct {
	SectorNum  uint64
	Phase1Out	[]byte
	SectorSize uint64
}