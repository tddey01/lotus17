package server_c2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

//var URL string

type WebRequest interface {
	Marshal() io.Reader
}

//type WebResponse interface {
//	Unmarshal(r io.Reader) error
//}

func RequestDo(rul string, router string, request WebRequest, response *Respones, timeout time.Duration) error {
	client := &http.Client{
		Timeout: timeout,
	}
	path := "http://" + rul + router
	var red io.Reader
	if request != nil {
		red = request.Marshal()
	}
	req, err := http.NewRequest("POST", path, red)
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return err
	}
	// 设置header
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	req.Header.Add("token", os.Getenv("C2_TOKEN"))
	// 在这里开始进行请求
	resp, err := client.Do(req)
	if err != nil {
		log.Println("请求错误：", err.Error())
		return err
	}
	defer resp.Body.Close()

	return response.Unmarshal(resp.Body)
}

func RequestDoByte(rul string, router string, request interface{}, response *Respones, timeout time.Duration) error {
	client := &http.Client{
		Timeout: timeout,
	}
	buf, err := json.Marshal(request)
	if err != nil {
		log.Println("序列化失败：", err.Error())
		return nil
	}
	path := "http://" + rul + router
	req, err := http.NewRequest("POST", path, bytes.NewReader(buf))
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return err
	}
	// 设置header
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	req.Header.Add("token", os.Getenv("C2_TOKEN"))
	// 在这里开始进行请求
	resp, err := client.Do(req)
	if err != nil {
		log.Println("请求错误：", err.Error())
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, &response)
}

var Token string

func RequestToDo(rul string, router string, request string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{
		Timeout: timeout,
	}
	//buf, err := json.Marshal(request)
	//if err != nil {
	//	log.Println("序列化失败：", err.Error())
	//	return nil, err
	//}
	path := "http://" + rul + router

	req, err := http.NewRequest("POST", path, bytes.NewReader([]byte(request)))
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return nil, err
	}

	// 设置header
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.FormatInt(req.ContentLength, 10))
	req.Header.Add("token", Token)
	// 在这里开始进行请求
	resp, err := client.Do(req)
	if err != nil {
		log.Println("请求错误：", err.Error())
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if http.StatusOK != resp.StatusCode {
		log.Println("返回错误：", string(body))
		return nil, fmt.Errorf("返回错误：%s", string(body))
	}
	return body, err
}
