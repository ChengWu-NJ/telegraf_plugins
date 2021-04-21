package net2

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/system"
	netutil "github.com/shirou/gopsutil/net"
)

type NetIOStats struct {
	filter filter.Filter
	ps     system.PS

	skipChecks bool
	//IgnoreProtocolStats bool
	Interfaces []string

	prevMetrics map[string]netutil.IOCountersStat
	preEpoch    int64
}

func (_ *NetIOStats) Description() string {
	return "Read metrics about network interface usage"
}

var netSampleConfig = `
  ## By default, telegraf gathers stats from any up interface (excluding loopback)
  ## Setting interfaces will tell it to gather these explicit interfaces,
  ## regardless of status.
  ##
  # interfaces = ["eth0"]
  ##
  ## On linux systems telegraf also collects protocol stats.
  ## Setting ignore_protocol_stats to true will skip reporting of protocol metrics.
  ##
  # ignore_protocol_stats = false
  ##
`

func (_ *NetIOStats) SampleConfig() string {
	return netSampleConfig
}

func (s *NetIOStats) Gather(acc telegraf.Accumulator) error {
	netio, err := s.ps.NetIO()
	if err != nil {
		return fmt.Errorf("error getting net io info: %s", err)
	}

	if s.filter == nil {
		if s.filter, err = filter.Compile(s.Interfaces); err != nil {
			return fmt.Errorf("error compiling filter: %s", err)
		}
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return fmt.Errorf("error getting list of interfaces: %s", err)
	}
	interfacesByName := map[string]net.Interface{}
	for _, iface := range interfaces {
		interfacesByName[iface.Name] = iface
	}

	nowEpoch := time.Now().UnixNano()
	gapNano := int64(0)
	if s.preEpoch > 0 {
		gapNano = nowEpoch - s.preEpoch
	}

	oneCounter := netutil.IOCountersStat{}
	tatalCounter := netutil.IOCountersStat{}

	for _, io := range netio {
		if len(s.Interfaces) != 0 {
			var found bool

			if s.filter.Match(io.Name) {
				found = true
			}

			if !found {
				continue
			}
		} else if !s.skipChecks {
			if strings.HasPrefix(io.Name, "ib") { //not collect ib cards
				continue
			}

			iface, ok := interfacesByName[io.Name]
			if !ok {
				continue
			}

			if iface.Flags&net.FlagLoopback == net.FlagLoopback {
				continue
			}

			if iface.Flags&net.FlagUp == 0 {
				continue
			}
		}

		tags := map[string]string{
			"interface": io.Name,
			"name":      io.Name, //redanduncy for adapt UI of Mali version
		}

		if gapNano > 0 {
			oneCounter.BytesSent = uint64(int64(io.BytesSent-
				s.prevMetrics[io.Name].BytesSent) * int64(1e9) / gapNano)
			oneCounter.BytesRecv = uint64(int64(io.BytesRecv-
				s.prevMetrics[io.Name].BytesRecv) * int64(1e9) / gapNano)
			oneCounter.PacketsSent = uint64(int64(io.PacketsSent-
				s.prevMetrics[io.Name].PacketsSent) * int64(1e9) / gapNano)
			oneCounter.PacketsRecv = uint64(int64(io.PacketsRecv-
				s.prevMetrics[io.Name].PacketsRecv) * int64(1e9) / gapNano)
			oneCounter.Errin = uint64(int64(io.Errin-
				s.prevMetrics[io.Name].Errin) * int64(1e9) / gapNano)
			oneCounter.Errout = uint64(int64(io.Errout-
				s.prevMetrics[io.Name].Errout) * int64(1e9) / gapNano)
			oneCounter.Dropin = uint64(int64(io.Dropin-
				s.prevMetrics[io.Name].Dropin) * int64(1e9) / gapNano)
			oneCounter.Dropout = uint64(int64(io.Dropout-
				s.prevMetrics[io.Name].Dropout) * int64(1e9) / gapNano)

			tatalCounter.BytesSent += oneCounter.BytesSent
			tatalCounter.BytesRecv += oneCounter.BytesRecv
			tatalCounter.PacketsSent += oneCounter.PacketsSent
			tatalCounter.PacketsRecv += oneCounter.PacketsRecv
			tatalCounter.Errin += oneCounter.Errin
			tatalCounter.Errout += oneCounter.Errout
			tatalCounter.Dropin += oneCounter.Dropin
			tatalCounter.Dropout += oneCounter.Dropout

			fields := map[string]interface{}{
				"sent_bytes":   io.BytesSent, //redanduncy for adapt UI of Mali version
				"recv_bytes":   io.BytesRecv, //redanduncy for adapt UI of Mali version
				"bytes_sent":   io.BytesSent,
				"bytes_recv":   io.BytesRecv,
				"packets_sent": io.PacketsSent,
				"packets_recv": io.PacketsRecv,
				"err_in":       io.Errin,
				"err_out":      io.Errout,
				"drop_in":      io.Dropin,
				"drop_out":     io.Dropout,
			}
			acc.AddCounter("net", fields, tags)
		}
		s.prevMetrics[io.Name] = io
	}

	if gapNano > 0 {
		totalTags := map[string]string{}
		totalFields := map[string]interface{}{
			"bytes_sent":   tatalCounter.BytesSent,
			"bytes_recv":   tatalCounter.BytesRecv,
			"write_total":  tatalCounter.BytesSent,
			"read_total":   tatalCounter.BytesRecv,
			"packets_sent": tatalCounter.PacketsSent,
			"packets_recv": tatalCounter.PacketsRecv,
			"err_in":       tatalCounter.Errin,
			"err_out":      tatalCounter.Errout,
			"drop_in":      tatalCounter.Dropin,
			"drop_out":     tatalCounter.Dropout,
		}
		acc.AddCounter("net_total", totalFields, totalTags)
	}
	s.preEpoch = nowEpoch

	// Get system wide stats for different network protocols
	// (ignore these stats if the call fails)
	/*
		if !s.IgnoreProtocolStats {
			netprotos, _ := s.ps.NetProto()
			fields := make(map[string]interface{})
			for _, proto := range netprotos {
				for stat, value := range proto.Stats {
					name := fmt.Sprintf("%s_%s", strings.ToLower(proto.Protocol),
						strings.ToLower(stat))
					fields[name] = value
				}
			}
			tags := map[string]string{
				"interface": "all",
			}
			acc.AddFields("net", fields, tags)
		}
	*/

	return nil
}

func init() {
	pm := map[string]netutil.IOCountersStat{}
	inputs.Add("net2", func() telegraf.Input {
		return &NetIOStats{ps: system.NewSystemPS(),
			prevMetrics: pm}
	})
}
