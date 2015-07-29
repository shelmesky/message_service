package utils

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"time"
)

var (
	Log *log.Logger
)

func GenKey(key string) uint32 {
	bKey := hashDigest(key)
	return hashVal(bKey[0:4])
}

func hashVal(bKey []byte) uint32 {
	return ((uint32(bKey[3]) << 24) |
		(uint32(bKey[2]) << 16) |
		(uint32(bKey[1]) << 8) |
		(uint32(bKey[0])))
}

func hashDigest(key string) []byte {
	m := md5.New()
	m.Write([]byte(key))
	return m.Sum(nil)
}

// 计算字符换的MD5
func MD5(text string) string {
	hashMD5 := md5.New()
	io.WriteString(hashMD5, text)
	return fmt.Sprintf("%x", hashMD5.Sum(nil))
}

// 获取随机id字符串
func MakeRandomID() string {
	nano := time.Now().UnixNano()
	rand.Seed(nano)
	rndNum := rand.Int63()

	md5_nano := MD5(strconv.FormatInt(nano, 10))
	md5_rand := MD5(strconv.FormatInt(rndNum, 10))
	return MD5(md5_nano + md5_rand)
}
