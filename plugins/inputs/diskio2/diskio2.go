package diskio2

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/filter"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/system"

	"github.com/shirou/gopsutil/disk"
)

var (
	varRegex = regexp.MustCompile(`\$(?:\w+|\{\w+\})`)
)

type DiskIO struct {
	ps system.PS

	Devices          []string
	DeviceTags       []string
	NameTemplates    []string
	SkipSerialNumber bool

	Log telegraf.Logger

	infoCache    map[string]diskInfoCache
	deviceFilter filter.Filter
	initialized  bool

	prevMetrics map[string]disk.IOCountersStat
	preEpoch    int64
}

func (_ *DiskIO) Description() string {
	return "Read metrics about disk IO by device"
}

var diskIOsampleConfig = `
  ## By default, telegraf will gather stats for all devices including
  ## disk partitions.
  ## Setting devices will restrict the stats to the specified devices.
  # devices = ["sda", "sdb", "vd*"]
  ## Uncomment the following line if you need disk serial numbers.
  # skip_serial_number = false
  #
  ## On systems which support it, device metadata can be added in the form of
  ## tags.
  ## Currently only Linux is supported via udev properties. You can view
  ## available properties for a device by running:
  ## 'udevadm info -q property -n /dev/sda'
  ## Note: Most, but not all, udev properties can be accessed this way. Properties
  ## that are currently inaccessible include DEVTYPE, DEVNAME, and DEVPATH.
  # device_tags = ["ID_FS_TYPE", "ID_FS_USAGE"]
  #
  ## Using the same metadata source as device_tags, you can also customize the
  ## name of the device via templates.
  ## The 'name_templates' parameter is a list of templates to try and apply to
  ## the device. The template may contain variables in the form of '$PROPERTY' or
  ## '${PROPERTY}'. The first template which does not contain any variables not
  ## present for the device is used as the device name tag.
  ## The typical use case is for LVM volumes, to get the VG/LV name instead of
  ## the near-meaningless DM-0 name.
  # name_templates = ["$ID_FS_LABEL","$DM_VG_NAME/$DM_LV_NAME"]
`

func (_ *DiskIO) SampleConfig() string {
	return diskIOsampleConfig
}

// hasMeta reports whether s contains any special glob characters.
func hasMeta(s string) bool {
	return strings.IndexAny(s, "*?[") >= 0
}

func (s *DiskIO) init() error {
	for _, device := range s.Devices {
		if hasMeta(device) {
			filter, err := filter.Compile(s.Devices)
			if err != nil {
				return fmt.Errorf("error compiling device pattern: %s", err.Error())
			}
			s.deviceFilter = filter
		}
	}
	s.initialized = true
	return nil
}

func (s *DiskIO) Gather(acc telegraf.Accumulator) error {
	if !s.initialized {
		err := s.init()
		if err != nil {
			return err
		}
	}

	devices := []string{}
	if s.deviceFilter == nil {
		devices = s.Devices
	}

	diskio, err := s.ps.DiskIO(devices)
	if err != nil {
		return fmt.Errorf("error getting disk io info: %s", err.Error())
	}

	nowEpoch := time.Now().UnixNano()
	gapNano := int64(0)
	if s.preEpoch > 0 {
		gapNano = nowEpoch - s.preEpoch
	}

	oneDiskCounter := disk.IOCountersStat{}
	tatalCounter := disk.IOCountersStat{}

	for _, io := range diskio {

		match := false
		if s.deviceFilter != nil && s.deviceFilter.Match(io.Name) {
			match = true
		}

		tags := map[string]string{}
		var devLinks []string
		tags["name"], devLinks = s.diskName(io.Name)

		if s.deviceFilter != nil && !match {
			for _, devLink := range devLinks {
				if s.deviceFilter.Match(devLink) {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}

		for t, v := range s.diskTags(io.Name) {
			tags[t] = v
		}

		if !s.SkipSerialNumber {
			if len(io.SerialNumber) != 0 {
				tags["serial"] = io.SerialNumber
			} else {
				tags["serial"] = "unknown"
			}
		}

		if gapNano > 0 {
			oneDiskCounter.ReadCount = uint64(int64(io.ReadCount-
				s.prevMetrics[io.Name].ReadCount) * int64(1e9) / gapNano)
			oneDiskCounter.WriteCount = uint64(int64(io.WriteCount-
				s.prevMetrics[io.Name].WriteCount) * int64(1e9) / gapNano)
			oneDiskCounter.ReadBytes = uint64(int64(io.ReadBytes-
				s.prevMetrics[io.Name].ReadBytes) * int64(1e9) / gapNano)
			oneDiskCounter.WriteBytes = uint64(int64(io.WriteBytes-
				s.prevMetrics[io.Name].WriteBytes) * int64(1e9) / gapNano)
			oneDiskCounter.ReadTime = uint64(int64(io.ReadTime-
				s.prevMetrics[io.Name].ReadTime) * int64(1e9) / gapNano)
			oneDiskCounter.WriteTime = uint64(int64(io.WriteTime-
				s.prevMetrics[io.Name].WriteTime) * int64(1e9) / gapNano)
			oneDiskCounter.IoTime = uint64(int64(io.IoTime-
				s.prevMetrics[io.Name].IoTime) * int64(1e9) / gapNano)
			oneDiskCounter.WeightedIO = uint64(int64(io.WeightedIO-
				s.prevMetrics[io.Name].WeightedIO) * int64(1e9) / gapNano)
			oneDiskCounter.IopsInProgress = uint64(int64(io.IopsInProgress-
				s.prevMetrics[io.Name].IopsInProgress) * int64(1e9) / gapNano)
			oneDiskCounter.MergedReadCount = uint64(int64(io.MergedReadCount-
				s.prevMetrics[io.Name].MergedReadCount) * int64(1e9) / gapNano)
			oneDiskCounter.MergedWriteCount = uint64(int64(io.MergedWriteCount-
				s.prevMetrics[io.Name].MergedWriteCount) * int64(1e9) / gapNano)

			tatalCounter.ReadCount += oneDiskCounter.ReadCount
			tatalCounter.WriteCount += oneDiskCounter.WriteCount
			tatalCounter.ReadBytes += oneDiskCounter.ReadBytes
			tatalCounter.WriteBytes += oneDiskCounter.WriteBytes
			tatalCounter.ReadTime += oneDiskCounter.ReadTime
			tatalCounter.WriteTime += oneDiskCounter.WriteTime
			tatalCounter.IoTime += oneDiskCounter.IoTime
			tatalCounter.WeightedIO += oneDiskCounter.WeightedIO
			tatalCounter.IopsInProgress += oneDiskCounter.IopsInProgress
			tatalCounter.MergedReadCount += oneDiskCounter.MergedReadCount
			tatalCounter.MergedWriteCount += oneDiskCounter.MergedWriteCount

			fields := map[string]interface{}{
				"reads":            oneDiskCounter.ReadCount,
				"writes":           oneDiskCounter.WriteCount,
				"read_bytes":       oneDiskCounter.ReadBytes,
				"write_bytes":      oneDiskCounter.WriteBytes,
				"read_time":        oneDiskCounter.ReadTime,
				"write_time":       oneDiskCounter.WriteTime,
				"io_time":          oneDiskCounter.IoTime,
				"weighted_io_time": oneDiskCounter.WeightedIO,
				"iops_in_progress": oneDiskCounter.IopsInProgress,
				"merged_reads":     oneDiskCounter.MergedReadCount,
				"merged_writes":    oneDiskCounter.MergedWriteCount,
			}
			acc.AddCounter("diskio", fields, tags)
		}
		s.prevMetrics[io.Name] = io
	}

	if gapNano > 0 {
		totalTags := map[string]string{}
		totalFields := map[string]interface{}{
			"reads":            tatalCounter.ReadCount,
			"writes":           tatalCounter.WriteCount,
			"read_bytes":       tatalCounter.ReadBytes,
			"write_bytes":      tatalCounter.WriteBytes,
			"read_total":       tatalCounter.ReadBytes,
			"write_total":      tatalCounter.WriteBytes,
			"read_time":        tatalCounter.ReadTime,
			"write_time":       tatalCounter.WriteTime,
			"io_time":          tatalCounter.IoTime,
			"weighted_io_time": tatalCounter.WeightedIO,
			"iops_in_progress": tatalCounter.IopsInProgress,
			"merged_reads":     tatalCounter.MergedReadCount,
			"merged_writes":    tatalCounter.MergedWriteCount,
		}
		acc.AddCounter("diskio_total", totalFields, totalTags)
	}
	s.preEpoch = nowEpoch

	return nil
}

func (s *DiskIO) diskName(devName string) (string, []string) {
	di, err := s.diskInfo(devName)
	devLinks := strings.Split(di["DEVLINKS"], " ")
	for i, devLink := range devLinks {
		devLinks[i] = strings.TrimPrefix(devLink, "/dev/")
	}

	mapperName, ok := di["DEVMAPPER"]
	if ok {
		devName = mapperName
	}

	if len(s.NameTemplates) == 0 {
		return devName, devLinks
	}

	if err != nil {
		s.Log.Warnf("Error gathering disk info: %s", err)
		return devName, devLinks
	}

	for _, nt := range s.NameTemplates {
		miss := false
		name := varRegex.ReplaceAllStringFunc(nt, func(sub string) string {
			sub = sub[1:] // strip leading '$'
			if sub[0] == '{' {
				sub = sub[1 : len(sub)-1] // strip leading & trailing '{' '}'
			}
			if v, ok := di[sub]; ok {
				return v
			}
			miss = true
			return ""
		})

		if !miss {
			return name, devLinks
		}
	}

	return devName, devLinks
}

func (s *DiskIO) diskTags(devName string) map[string]string {
	if len(s.DeviceTags) == 0 {
		return nil
	}

	di, err := s.diskInfo(devName)
	if err != nil {
		s.Log.Warnf("Error gathering disk info: %s", err)
		return nil
	}

	tags := map[string]string{}
	for _, dt := range s.DeviceTags {
		if v, ok := di[dt]; ok {
			tags[dt] = v
		}
	}

	return tags
}

func init() {
	ps := system.NewSystemPS()
	pm := map[string]disk.IOCountersStat{}
	inputs.Add("diskio2", func() telegraf.Input {
		return &DiskIO{ps: ps, SkipSerialNumber: true,
			prevMetrics: pm}
	})
}
