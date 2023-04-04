package server_c2

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type SealerParam struct {
	Sector    storiface.SectorRef
	Phase1Out storiface.Commit1Out
	Status  int
}

func (s *SealerParam) Marshal() io.Reader {
	buf, err := json.Marshal(*s)
	if err != nil {
		log.Println("序列化失败：", err.Error())
		return nil
	}
	return bytes.NewReader(buf)
}
func (s *SealerParam) Unmarshal(r io.Reader) error {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, s)
}

func (s *SealerParam)SealCommit2(url string)(Respones,error){
	var res Respones
	if err := RequestDo(url,"/runcommit2",s,&res,time.Minute*4);err!=nil{
		return res,err
	}
	if http.StatusOK!=res.Code&&res.Code!=0&&http.StatusCreated!=res.Code{
		return res,errors.New(res.Data.(string))
	}
	return res,nil
}

