package server_c2

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type ProofResult struct {
	Proof storiface.Proof `json:"proof"`
	Err   string          `json:"err"`
}

func (s *ProofResult) Marshal() io.Reader {
	buf, err := json.Marshal(*s)
	if err != nil {
		log.Println("序列化失败：", err.Error())
		return nil
	}
	return bytes.NewReader(buf)
}
func (s *ProofResult) Unmarshal(r io.Reader) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, s)
}

func (s *ProofResult) GetCommit2(sid abi.SectorID, url string) error {
	var res Respones
	if err := RequestDoByte(url, "/getcommit2", sid, &res, time.Second*15); err != nil {
		return err
	}
	if http.StatusOK != res.Code {
		return errors.New(res.Data.(string))
	}
	buf, err := json.Marshal(res.Data)
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, s)
}
