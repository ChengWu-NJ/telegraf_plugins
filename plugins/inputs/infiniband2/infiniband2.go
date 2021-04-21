package infiniband2

import (
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

//Description -- this is necessary for telegraf plugins framework
func (s *Infiniband2) Description() string {
	return "Read metrics about Infiniband HCAs"
}

//SampleConfig --
func (s *Infiniband2) SampleConfig() string {
	return `
  ## default gather metrics on all ports. 
  ## set ignor_down_port = true if you wanna filter out down ports.
  # ignor_down_port = true
  ## default rescan hcas at 100 intervals to synchronise changes.
  ## set scan_after_intervals to new value if you wanna other rescan interval.
  # scan_after_intervals = 100
    `
}

//BASEDIR - the sysfs entry for infiniband device
const BASEDIR = "/sys/class/infiniband/"

//DEFAULTLUN -- the number of lun of hca port
const DEFAULTLUN = 4

//IBPort -- Ports of HCA card
type IBPort struct {
	ConfigfsPath string
	CA           string
	Port         string
	State        string
	PhysState    string
	RateGB       int64
	lun          int64
	SendBytes    int64
	RecvBytes    int64

	NowXmitData  int64
	NowRecvData  int64
	NowEpoch     int64
	LastXmitData int64
	LastRecvData int64
	LastEpoch    int64
}

//Infiniband2 --
type Infiniband2 struct {
	ports               map[string]*IBPort
	IgnorDownPort       bool  `toml:"ignor_down_port"`
	ScanAfterIntervals  int64 `toml:"scan_after_intervals"`
	ScanIntervalCounter int64
	initialized         bool
}

//ExtractIBRate --- get a GB rate and number of luns
func ExtractIBRate(portratestr string) (int64, int64) {
	defer func() {
		recover() //recover from panic when no matches
	}()

	re := regexp.MustCompile(`[\d]*[\.]?\d[\d]*`)
	matches := re.FindAllString(portratestr, -1)
	rate, _ := strconv.ParseInt(matches[0], 10, 0)
	lun, _ := strconv.ParseInt(matches[1], 10, 0)
	return rate, lun
}

//Gather -- gather metrics on one port
func (s *IBPort) Gather() error {
	content, err := ioutil.ReadFile(s.ConfigfsPath + "/state")
	if err != nil {
		return err
	}
	s.State = strings.TrimSpace(strings.Split(fmt.Sprintf("%s", content), " ")[1])

	content, err = ioutil.ReadFile(s.ConfigfsPath + "/phys_state")
	if err != nil {
		return err
	}
	s.PhysState = strings.TrimSpace(strings.Split(fmt.Sprintf("%s", content), " ")[1])

	content, err = ioutil.ReadFile(s.ConfigfsPath + "/rate")
	if err != nil {
		return err
	}
	s.RateGB, s.lun = ExtractIBRate(fmt.Sprintf("%s", content))

	content, err = ioutil.ReadFile(s.ConfigfsPath + "/counters/port_xmit_data")
	if err != nil {
		return err
	}
	s.LastXmitData = s.NowXmitData
	s.NowXmitData, err = strconv.ParseInt(
		strings.TrimSpace(fmt.Sprintf("%s", content)), 10, 64)
	if err != nil {
		return err
	}

	content, err = ioutil.ReadFile(s.ConfigfsPath + "/counters/port_rcv_data")
	s.LastRecvData = s.NowRecvData
	s.NowRecvData, err = strconv.ParseInt(
		strings.TrimSpace(fmt.Sprintf("%s", content)), 10, 64)
	if err != nil {
		return err
	}

	s.LastEpoch = s.NowEpoch
	s.NowEpoch = time.Now().UnixNano()

	return nil
}

//CalcRate -- calculate speed of send and receive
func (s *IBPort) CalcRate() {
	var numLun int64
	if interval := (s.NowEpoch - s.LastEpoch) / 1.e9; interval > 0 {
		if s.lun > 0 {
			numLun = s.lun
		} else {
			numLun = DEFAULTLUN
		}
		s.SendBytes = int64((s.NowXmitData - s.LastXmitData) *
			numLun / interval)
		s.RecvBytes = int64((s.NowRecvData - s.LastRecvData) *
			numLun / interval)
	} else {
		s.SendBytes = 0
		s.RecvBytes = 0
	}
}

//init -- init ports
func (s *Infiniband2) init() error {
	//clear all ports for rescan
	for k := range s.ports {
		delete(s.ports, k)
	}

	cas, err := ioutil.ReadDir(BASEDIR)
	if err != nil {
		return err
	}

	for _, ca := range cas {
		curpath := BASEDIR + ca.Name() + "/ports/"
		portpaths, err := ioutil.ReadDir(curpath)
		if err != nil {
			return err
		}

		for _, portpath := range portpaths {
			curpath = BASEDIR + ca.Name() + "/ports/" + portpath.Name()
			tmpPort := IBPort{ConfigfsPath: curpath, CA: ca.Name(), Port: portpath.Name()}
			err = tmpPort.Gather()
			if err != nil {
				return err
			}
			s.ports[tmpPort.ConfigfsPath] = &tmpPort
		}
	}

	s.initialized = true
	return nil
}

//rescan -- rescan hcas to synchronise changes
func (s *Infiniband2) rescan() error {
	tmpports := make(map[string]*IBPort)

	for k := range s.ports {
		tmpport := *(s.ports[k]) //copy struct
		tmpports[k] = &tmpport
	}

	err := s.init()
	if err != nil {
		return err
	}
	for k := range s.ports {
		if tmpports[k] != nil {
			*s.ports[k] = *tmpports[k]
		}
	}

	return nil
}

//FlushOut -- call Accumulator to output
func (s *Infiniband2) FlushOut(acc *telegraf.Accumulator, port *IBPort) {
	tags := map[string]string{}
	tags["caname"] = port.CA
	tags["port"] = port.Port
	tags["state"] = port.State
	tags["phys_state"] = port.PhysState

	fields := map[string]interface{}{
		"rate_gb":    port.RateGB,
		"send_bytes": port.SendBytes,
		"recv_bytes": port.RecvBytes,
	}

	(*acc).AddFields("infiniband", fields, tags)
}

//Gather -- invoked by telegraf at intervals
func (s *Infiniband2) Gather(acc telegraf.Accumulator) error {
	s.ScanIntervalCounter++
	s.ScanIntervalCounter = int64(math.Mod(
		float64(s.ScanIntervalCounter), float64(s.ScanAfterIntervals)))
	if s.ScanIntervalCounter == 0 {
		s.initialized = false
	}

	if !s.initialized {
		err := s.rescan()
		if err != nil {
			return err
		}
	}

	for _, port := range s.ports {
		err := port.Gather()
		if err != nil {
			return err
		}
		port.CalcRate()

		if !s.IgnorDownPort || port.State != "DOWN" {
			s.FlushOut(&acc, port)
		}
	}
	return nil
}

// Initialise plugin

func init() {
	ports := make(map[string]*IBPort)
	inputs.Add("infiniband2",
		func() telegraf.Input {
			return &Infiniband2{
				ports:              ports,
				IgnorDownPort:      false,
				ScanAfterIntervals: 100,
			}
		})
}
