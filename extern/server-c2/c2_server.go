package server_c2

import (
	"errors"
	"net/http"
	"time"
)

type C2Server struct{
	Url string
	GpuCount int
}

func (c *C2Server)UpdateGpuCount()error{
	var count Respones
	if err := RequestDo(c.Url,"/gpucount",nil,&count,time.Second*15);err!=nil{
		return err
	}
	if count.Code!=http.StatusOK{
		return errors.New(count.Data.(string))
	}
	c.GpuCount = int(count.Data.(float64))
	return nil
}
//func (s *C2Server) Marshal() io.Reader {
//	buf, err := json.Marshal(*s)
//	if err != nil {
//		log.Println("序列化失败：", err.Error())
//		return nil
//	}
//	return bytes.NewReader(buf)
//}
//func (s *C2Server) Unmarshal(r io.Reader) error {
//	body, err := ioutil.ReadAll(r)
//	if err != nil {
//		return err
//	}
//
//	return json.Unmarshal(body, s)
//}

