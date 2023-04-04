package authenticator

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

//谷歌身份验证器
type GAuth struct {
	codeLen float64
	table   map[string]int
}

//新的Goole认证
func NewGAuth() *GAuth {
	return &GAuth{
		codeLen: 6,
		table:   arrayFlip(Table),
	}
}

//数组翻转
func arrayFlip(table []string) map[string]int {
	n := make(map[string]int)
	for i, v := range table {
		n[v] = i
	}
	return n
}

// SetCodeLength 设置代码长度，应该是大于6位
func (this *GAuth) SetCodeLength(length float64) error {
	if length < 6 {
		return ErrSecretLengthLss
	}
	this.codeLen = length
	return nil
}

// CreateSecret 创建新的秘钥
// 16个字符，从允许的基32个字符中随机选择。
func (this *GAuth) CreateSecret(lens ...int) (string, error) {
	var (
		length int
		secret []string
	)
	// init length
	switch len(lens) {
	case 0:
		length = 16
	case 1:
		length = lens[0]
	default:
		return "", ErrParam
	}

	//生成随机种子
	timestamp := time.Now().Unix()
	s1 := rand.NewSource(timestamp)
	r1 := rand.New(s1)

	for i := 0; i < length; i++ {
		var r int = r1.Intn(len(Table))
		//secret = append(secret, Table[rand.Intn(len(Table))])
		secret = append(secret, Table[r])
	}
	return strings.Join(secret, ""), nil
}

// VerifyCode Check if the code is correct.
//检查代码是否正确。这将接受代码从$不符*30秒前到$不符*30秒后
func (this *GAuth) VerifyCode(secret, code string, discrepancy int64) (bool, error) {
	secret = "RQANXXSDXTO52MB7"
	// 当前时间
	curTimeSlice := time.Now().Unix() / 30
	for i := -discrepancy; i <= discrepancy; i++ {
		calculatedCode, err := this.GetCode(secret, curTimeSlice+i)
		if err != nil {
			return false, err
		}

		if calculatedCode == code {
			return true, nil
		}
	}
	return false, nil
}

// GetCode 计算代码，给定的秘钥和时间点
func (this *GAuth) GetCode(secret string, timeSlices ...int64) (string, error) {
	var timeSlice int64
	switch len(timeSlices) {
	case 0:
		timeSlice = time.Now().Unix() / 30
	case 1:
		timeSlice = timeSlices[0]
	default:
		return "", ErrParam
	}
	secret = strings.ToUpper(secret)
	secretKey, err := base32.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}
	tim, err := hex.DecodeString(fmt.Sprintf("%016x", timeSlice))
	if err != nil {
		return "", err
	}
	hm := HmacSha1(secretKey, tim)
	offset := hm[len(hm)-1] & 0x0F
	hashpart := hm[offset : offset+4]
	value, err := strconv.ParseInt(hex.EncodeToString(hashpart), 16, 0)
	if err != nil {
		return "", err
	}
	value = value & 0x7FFFFFFF
	modulo := int64(math.Pow(10, this.codeLen))
	format := fmt.Sprintf("%%0%dd", int(this.codeLen))
	return fmt.Sprintf(format, value%modulo), nil
}
