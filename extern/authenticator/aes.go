package authenticator

import (
	"crypto/aes"
	"crypto/cipher"
)

//var key = []byte("yungo@2021-04-16")
//var iv = []byte("yungo-2020-06-16") //初始化向量
// 使用 AES 加密算法 CTR 分组密码模式 加密
func AesEncrypt(plainText []byte) []byte {
	// 创建底层 aes 加密算法接口对象
	keystr := " "
	ivstr := ""
	keys := make([]byte, 16)
	ivs := make([]byte, 16)
	for i := 0; i < 16; i++ {
		if i%2 == 0 {
			keys[i] = keystr[i/2]
			ivs[i] = ivstr[i/2]
		} else {
			keys[i] = Zckey[i/2]
			ivs[i] = Zciv[i/2]
		}
	}
	block, err := aes.NewCipher(keys)
	if err != nil {
		panic(err)
	}
	// 创建 CTR 分组密码模式 接口对象
	//iv := []byte("12345678abcdefgh")			// == 分组数据长度 16
	stream := cipher.NewCTR(block, ivs)

	// 加密
	stream.XORKeyStream(plainText, plainText)
	return plainText
}

// 使用 AES 加密算法 CTR 分组密码模式 解密
func AesDecrypt(cipherText []byte) []byte {
	keystr := ""
	ivstr := ""
	keys := make([]byte, 16)
	ivs := make([]byte, 16)
	for i := 0; i < 16; i++ {
		if i%2 == 0 {
			keys[i] = keystr[i/2]
			ivs[i] = ivstr[i/2]
		} else {
			keys[i] = Zckey[i/2]
			ivs[i] = Zciv[i/2]
		}
	}
	// 创建底层 des 加密算法接口对象
	block, err := aes.NewCipher(keys)
	if err != nil {
		panic(err)
	}
	// 创建 CBC 分组密码模式 接口
	//iv := []byte("12345678abcdefgh")			// == 分组数据长度 16
	stream := cipher.NewCTR(block, ivs)

	// 解密
	stream.XORKeyStream(cipherText, cipherText)
	return cipherText
}
