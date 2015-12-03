/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Chance Zibolski (chance.zibolski@gmail.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package heka_sftp_polling_input

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SftpPollingInput struct {
	*SftpPollingInputConfig
	stop     chan bool
	runner   pipeline.InputRunner
	hostname string
	client   *sftp.Client
	conn     *ssh.Client
}

type SftpPollingInputConfig struct {
	TickerInterval uint   `toml:"ticker_interval"`
	Repeat         bool   `toml:"repeat"`
	RepeatMarkDir  string `toml:"repeat_mark_dir"`
	Suffix         string `toml:"suffix"`
	MaxPacket      int    `toml:"max_packet"`
	Host           string `toml:"host"`
	Port           int    `toml:"port"`
	User           string `toml:"user"`
	Pass           string `toml:"password"`
	Compress       bool   `toml:"compress"`
	Dir            string `toml:"dir"`
}

func (input *SftpPollingInput) ConfigStruct() interface{} {
	return &SftpPollingInputConfig{
		TickerInterval: uint(5),
		Repeat:         false,
		MaxPacket:      1 << 15,
		Port:           22,
		RepeatMarkDir:  "dir_polling_input_repeat_mark",
	}
}

func (input *SftpPollingInput) Init(config interface{}) (err error) {
	conf := config.(*SftpPollingInputConfig)
	input.SftpPollingInputConfig = conf
	input.stop = make(chan bool)
	addr := fmt.Sprintf("%s:%d", conf.Host, conf.Port)

	var auths []ssh.AuthMethod

	if conf.Pass != "" {
		auths = append(auths, ssh.Password(conf.Pass))
	}

	sss_config := ssh.ClientConfig{
		User: conf.User,
		Auth: auths,
	}

	if input.conn, err = ssh.Dial("tcp", addr, &sss_config); err == nil {
		if input.client, err = sftp.NewClient(input.conn, sftp.MaxPacket(conf.MaxPacket)); err == nil {
		} else {
			log.Fatalf("unable to start sftp subsytem: %v", err)
		}
	} else {
		log.Fatalf("unable to connect to [%s]: %v", addr, err)
	}
	return
}

func (input *SftpPollingInput) Stop() {
	input.conn.Close()
	input.client.Close()
	close(input.stop)
}

func (input *SftpPollingInput) packDecorator(pack *pipeline.PipelinePack) {
	pack.Message.SetType("heka.sftp.polling")
}

func (input *SftpPollingInput) Run(runner pipeline.InputRunner,
	helper pipeline.PluginHelper) error {

	input.runner = runner
	input.hostname = helper.PipelineConfig().Hostname()
	tickChan := runner.Ticker()
	sRunner := runner.NewSplitterRunner("")
	if !sRunner.UseMsgBytes() {
		sRunner.SetPackDecorator(input.packDecorator)
	}

	for {
		select {
		case <-input.stop:
			return nil
		case <-tickChan:
		}

		runner.LogMessage("start polling from sftp")
		if err := input.polling(func(f io.ReadSeeker, name string) error {
			return input.read(f, name, runner)
		}); err != nil {
			runner.LogError(err)
			return nil
		}
	}
}

func (input *SftpPollingInput) polling(fn func(io.ReadSeeker, string) error) error {
	if infos, err := input.client.ReadDir(input.Dir); err == nil {
		for _, info := range infos {
			if (!info.IsDir()) && (input.SftpPollingInputConfig.Suffix == "" || strings.HasSuffix(info.Name(), input.SftpPollingInputConfig.Suffix)) {
				mark := filepath.Join(input.SftpPollingInputConfig.RepeatMarkDir, input.Dir, info.Name()+".ok")
				if !input.SftpPollingInputConfig.Repeat {
					if _, err := os.Stat(mark); !os.IsNotExist(err) {
						return fmt.Errorf("repeat file %s", info.Name())
					}
				}
				dir := filepath.Dir(mark)
				if err = os.MkdirAll(dir, 0774); err != nil {
					return fmt.Errorf("Error opening file: %s", err.Error())
				} else {
					if err := ioutil.WriteFile(mark, []byte(time.Now().String()), 0664); err != nil {
						return err
					}
				}

				f, err := input.client.Open(filepath.Join(input.Dir, info.Name()))

				if err != nil {
					return fmt.Errorf("Error opening remote file: %s", err.Error())
				}

				defer f.Close()
				err = fn(f, info.Name())
			}
		}
	}
	return nil
}

func (input *SftpPollingInput) read(file io.ReadSeeker, name string, runner pipeline.InputRunner) (err error) {
	sRunner := runner.NewSplitterRunner(name)
	if !sRunner.UseMsgBytes() {
		sRunner.SetPackDecorator(func(pack *pipeline.PipelinePack) {
			pack.Message.SetType("heka.sftp.polling")
			pack.Message.SetHostname(input.hostname)
			if field, err := message.NewField("file_name", name, ""); err == nil {
				pack.Message.AddField(field)
			}
			return
		})
	}

	if input.SftpPollingInputConfig.Compress && input.SftpPollingInputConfig.Suffix == ".gz" {

		var fileReader *gzip.Reader
		if fileReader, err = gzip.NewReader(file); err == nil {
			defer fileReader.Close()
			tarBallReader := tar.NewReader(fileReader)
			for {
				if header, err := tarBallReader.Next(); err == nil {
					// get the individual filename and extract to the current directory
					switch header.Typeflag {
					case tar.TypeReg:
						split(runner, sRunner, tarBallReader)
					}
				} else {
					break
				}
			}
		}
	} else {
		split(runner, sRunner, file)
	}
	return
}

func split(runner pipeline.InputRunner, sRunner pipeline.SplitterRunner, reader io.Reader) {
	var err error
	for err == nil {
		err = sRunner.SplitStream(reader, nil)
		if err != io.EOF && err != nil {
			runner.LogError(fmt.Errorf("Error reading file: %s", err.Error()))
		}
	}
}

func init() {
	pipeline.RegisterPlugin("SftpPollingInput", func() interface{} {
		return new(SftpPollingInput)
	})
}
