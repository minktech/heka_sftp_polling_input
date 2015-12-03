package heka_sftp_polling_input

import (
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

func TestPolling(t *testing.T) {
	conf := SftpPollingInputConfig{
		Host:          "",
		Port:          22,
		User:          "",
		Pass:          "",
		Dir:           "/root",
		MaxPacket:     1 << 15,
		RepeatMarkDir: "dir_polling_input_repeat_mark",
	}
	input := SftpPollingInput{}
	input.Init(&conf)
	err := input.polling(func(f io.ReadSeeker, name string) error {
		bts, err := ioutil.ReadAll(f)
		fmt.Println(string(bts), err)
		return nil
	})
	fmt.Println(err)
}
