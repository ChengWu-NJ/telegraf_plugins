package bashpool

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

const (
	sampleConfig = `
### there can be multiple usershells in a bashpool
[[inputs.bashpool.user_shell]]
        ### id must be unique
        id = "root1"
        os_user = "root"

### command script must be assigned to a certian usershell
[[inputs.bashpool.cmd_define]]
        ### id must be unique
        id = "hostbaseinfo"
        shell_id = "root1"
        interval_second = 180
        tags = ["cmdb_id = cmdb001122"]

        cmd_line = '''
echo hostbaseinfo ntp_chronyc=\"$(ntp_chronyc=$(timedatectl|grep -i synchronized|grep -i yes);if (( ${#ntp_chronyc} > 0 ));then echo "sync";else echo "no sync";fi)\",cpu_module=\"$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)\",cpu_cores=\"$(cat /proc/cpuinfo|grep processor|wc -l)\",cpu_type=\"$(cat /proc/cpuinfo|grep 'model name'|uniq|grep -Po \(?\<=:\ \).*\\b)\",os=\"$(cat /etc/system-release)\",firewalld=\"$(if [[ $(systemctl is-active firewalld) == 'active' ]];then echo 'running';else echo 'stopped';fi)/$(systemctl is-enabled firewalld)\",networkManager=\"$(if [[ $(systemctl is-active NetworkManager) == 'active' ]];then echo 'running';else echo 'stopped';fi)/$(systemctl is-enabled NetworkManager)\",mem=$(($(cat /proc/meminfo|grep -Po \(?\<=MemTotal\:\).*\\d)*1024))i,numa=\"$(a=$(dmesg|grep -i numa|grep -Poi "no.*numa.*found");if (( ${#a} > 0 ));then echo disabled;else echo enabled;fi)\",timezone=\"$(timedatectl|grep -Poi \(?\<=Time\ zone:\ \).*\(?=\ \\\(\))\",selinux=\"$(getenforce)\"
        '''

[[inputs.bashpool.cmd_define]]
        ### id must be unique
        id = "flash"
        shell_id = "root1"
        interval_second = 180
        tags = ["cmdb_id = cmdb002233"]
        cmd_line = '''
nvme list -o json|grep -Po \(?\<=\"DevicePath\"\ :\ \"\).*\(?=\"\)|xargs -I{} bash -c 'echo flash,path={} used_percent=\"$(nvme smart-log {}|grep percentage_used|grep -Po \(?\<=:\ \).*)\"'
'''
`
)

//SampleConfig -- input interface function
func (B *BashPool) SampleConfig() string {
	return sampleConfig
}

//Description -- input interface function
func (B *BashPool) Description() string {
	return `
#bashpool -- a service to provide a pool of shells of linux bash
#  to run script in designed linux user account, and relay stdout
#  to output plugin.
`
}

//MSGType -- for send msg by channels of different kinds
type MSGType byte

const (
	//OTStdout -- out type for stdout. in current, only support influxdb format
	OTStdout MSGType = iota
	//OTStderr -- out type for stderr. maybe will not be used
	OTStderr
	//OTEOFStdOut -- end of stdout after finish executing command
	OTEOFStdOut
	//OTEOFStdErr -- end of stderr after finish executing command
	OTEOFStdErr
	//OTProbe -- for probe to make sure shell is working
	OTProbe

	//ProbeStr --
	ProbeStr = `!@#PROBE#@!`

	SEPERATOR = `[@S#P#T@]`
)

//MSGLine -- for sending msg to telegraf output or internal communication
type MSGLine struct {
	CmdDefID string
	Type     MSGType
	Line     string
}

//CmdDef -- bash script define
type CmdDef struct {
	ID          string   `toml:"id"` //unique
	ShellID     string   `toml:"shell_id"`
	IntervalSec uint     `toml:"interval_second"`
	CmdLine     string   `toml:"cmd_line"`
	Tags        []string `toml:"tags"`

	bashPool *BashPool
}

//UserShell -- exec.Cmd with os user
type UserShell struct {
	ID         string `toml:"id"`
	OSUser     string `toml:"os_user"`
	Cmd        *exec.Cmd
	CmdReady   bool //ok after su - user
	readyCh    chan bool
	StdinPipe  io.WriteCloser
	StdoutPipe io.ReadCloser
	StderrPipe io.ReadCloser

	*sync.Mutex
	//probeCh  chan bool
	bashPool *BashPool
}

// BashPool is an input plugin that collects external metrics sent via HTTP
type BashPool struct {
	OutCh          chan MSGLine
	ConfUserShells []*UserShell          `toml:"user_shell"`
	ConfCmdDefs    []*CmdDef             `toml:"cmd_define"`
	UserShells     map[string]*UserShell //key by id
	CmdDefs        map[string]*CmdDef

	parser    parsers.Parser
	Log       telegraf.Logger
	wg        sync.WaitGroup
	acc       telegraf.Accumulator
	ScriptDir string
}

//initUserShell -- create UserShell and add to pool
func (US *UserShell) startUserShell(ctx context.Context, acc telegraf.Accumulator) error {
	shell := US.Cmd
	if shell == nil {
		err := US.startNewExecCmd(ctx)
		if err != nil {
			return err
		}
	}
	US.bashPool.Log.Debugf("finish startNewExecCmd [%s]", US.ID)

	US.bashPool.wg.Add(1)
	go func() {
		defer func() {
			US.bashPool.Log.Debugf("stroutpipe routine exited")
			US.bashPool.wg.Done()
		}()

		var line string

		scan := bufio.NewScanner(US.StdoutPipe)
		scan.Split(bufio.ScanLines)
		US.bashPool.Log.Debugf("[%s] Stdout scanner ready", US.ID)
		US.bashPool.Log.Debugf("before US.readyCh <- true")
		US.readyCh <- true
		US.bashPool.Log.Debugf("after US.readyCh <- true")

		for {
			for scan.Scan() {
				line = scan.Text()
				US.bashPool.Log.Debugf("[%s] stdoutline:[%s]", US.ID, line)
				//ignore cmd of suing to create usershell and probe
				if line == ProbeStr {
					continue
				}

				lst := strings.Split(line, SEPERATOR)
				if len(lst) <= 1 {
					continue //go on if no cmddef id
				}

				cmdDefID := lst[0]
				line = lst[1]
				metrics, err := US.bashPool.parser.Parse([]byte(line))
				if err != nil {
					acc.AddError(err)
					continue
				}

				for _, m := range metrics {
					//m.AddTag()
					cDef, ok := US.bashPool.CmdDefs[cmdDefID]
					if ok {
						for _, t := range cDef.Tags {
							kv := strings.Split(t, "=")
							if len(kv) > 1 {
								m.AddTag(strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1]))
							}
						}
					}

					acc.AddMetric(m)
				}
			}

			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}
	}()

	US.bashPool.wg.Add(1)
	go func() {
		defer func() {
			US.bashPool.Log.Debugf("strERRpipe routine exited")
			US.bashPool.wg.Done()
		}()

		var line string

		scan := bufio.NewScanner(US.StderrPipe)
		scan.Split(bufio.ScanLines)
		//ignore all stderr
		for {
			for scan.Scan() {
				line = scan.Text()
				US.bashPool.Log.Debugf("[%s] stderrline:[%s]", US.ID, line)
			}

			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}

	}()

	US.bashPool.Log.Debugf("before <-readyCh")
	<-US.readyCh
	US.bashPool.Log.Debugf("after <-readyCh")
	US.CmdReady = true

	return nil
}

//NewUserShell -- create UserShell and add to pool
func (US *UserShell) startNewExecCmd(ctx context.Context) error {
	US.Lock()
	defer US.Unlock()

	cmd := exec.CommandContext(ctx, "bash")

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	US.Cmd = cmd
	US.StderrPipe = stderrPipe
	US.StdinPipe = stdinPipe
	US.StdoutPipe = stdoutPipe

	US.CmdReady = false
	err = US.Cmd.Start()
	if err != nil {
		return err
	}
	_, err = io.WriteString(US.StdinPipe, "su - "+US.OSUser+"\n")
	if err != nil {
		return err
	}

	return nil
}

//in condition of new task waiting to run
func (US *UserShell) probe(ctx context.Context) error {
	ch := make(chan error, 1)
	defer close(ch)

	probeCmd := "echo " + ProbeStr
	go func() {
		US.Lock()
		n, err := io.WriteString(US.StdinPipe, probeCmd+"\n")
		US.bashPool.Log.Debugf("[%s] StdinPipe n=%d, err=[%+v]\n[%s]", US.ID, n, err, probeCmd)
		US.Unlock()
		if n == 0 && err != nil { //stdinpipe is broken. need recreate usershell
			US.bashPool.Log.Debugf("[%s] probe failed, got err:[%+v]", US.ID, err)
			ch <- US.startNewExecCmd(ctx)
		} else {
			ch <- nil
		}
	}()

	//timer := time.NewTimer(100 * time.Millisecond)
	//defer timer.Stop()

	select {
	case errch := <-ch:
		return errch

	//case <-timer.C:
	//	return fmt.Errorf("usershell %s did not respond probe in 100 ms", US.ID)

	case <-ctx.Done():
		return nil
	}
}

func (c *CmdDef) prepareCmd() error {
	var err error
	f := fmt.Sprintf("%s/%s.telegraf", c.bashPool.ScriptDir, c.ID)

	//save cmdline to /dev/shm/pid/
	err = ioutil.WriteFile(f, []byte(c.CmdLine), 0644)
	if err != nil {
		err = fmt.Errorf("failed to save content of CmdDef [%s] to %s, got err:[%s]", c.ID, c.bashPool.ScriptDir, err.Error())
	}

	return err
}

func (US *UserShell) sendScript(ctx context.Context, c *CmdDef) error {

	if err := US.probe(ctx); err != nil {
		return fmt.Errorf("%s. usershell %s is getting a trouble", err, US.ID)
	}

	if err := c.prepareCmd(); err != nil {
		return err
	}

	US.Lock()
	_, _ = io.WriteString(US.StdinPipe, "exec bash"+"\n")
	realCmd := fmt.Sprintf(`source %s/%s.telegraf|awk '{printf "%s%s%%s\n",$0}'`,
		c.bashPool.ScriptDir, c.ID, c.ID, SEPERATOR)
	n, err := io.WriteString(US.StdinPipe, realCmd+"\n")
	US.bashPool.Log.Debugf("[%s] StdinPipe n=%d, err=[%+v]\n[%s]", US.ID, n, err, realCmd)
	US.Unlock()

	return err
}

func (US *UserShell) gatherOnInterval(ctx context.Context, c *CmdDef) {
	defer func() {
		US.bashPool.Log.Debugf("gatherOnInterval exited")
		US.bashPool.wg.Done()
	}()

	//start all gathering items randomly in 10s
	rand.Seed(time.Now().UnixNano())
	r := rand.Float64()
	time.Sleep(time.Duration(r*10000) * time.Millisecond)

	interval := time.Duration(c.IntervalSec) * time.Second

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if US.CmdReady {
			err := US.sendScript(ctx, c)
			if err != nil {
				US.bashPool.acc.AddError(err)
			}
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			US.bashPool.Log.Debugf("get ctx.Done")
			return
		}
	}
}

func (B *BashPool) runCmdDefs(ctx context.Context) error {
	//appoint usershell to cmddefs and begin tick to run
	for _, c := range B.CmdDefs {

		ushell := B.UserShells[c.ShellID]
		if ushell == nil {
			return fmt.Errorf("Cannot get usershell %s from pool to appoint to cmd_define %s",
				c.ShellID, c.ID)
		}

		B.wg.Add(1)
		go ushell.gatherOnInterval(ctx, c)
		//c.userShell = ushell
	}

	return nil
}

//init -- toml config --> slice --> map
func (B *BashPool) init() error {
	bConfShell := false
	for _, cs := range B.ConfUserShells {

		cs.bashPool = B
		cs.Mutex = &sync.Mutex{}
		cs.readyCh = make(chan bool, 1)
		B.UserShells[cs.ID] = cs

		bConfShell = true
	}
	if !bConfShell {
		return fmt.Errorf("none of usershell defined in config file")
	}

	bConfCmdDef := false
	for _, cc := range B.ConfCmdDefs {

		cc.bashPool = B
		B.CmdDefs[cc.ID] = cc

		bConfCmdDef = true
	}
	if !bConfCmdDef {
		return fmt.Errorf("none of CmdDef defined in config file")
	}

	return nil
}

// StartContext -- start the service. invoked by telegraf framwork
func (B *BashPool) StartContext(ctx context.Context, acc telegraf.Accumulator) error {
	//check dir of scripts
	pid := os.Getpid()
	B.ScriptDir = fmt.Sprintf("/dev/shm/%d", pid)
	if _, err := os.Stat(B.ScriptDir); err != nil {
		err := os.Mkdir(B.ScriptDir, 0644)
		if err != nil {
			err = fmt.Errorf("failed to mkdir [%s]", B.ScriptDir)
			return err
		}
	}

	if err := B.init(); err != nil {
		return err
	}
	B.Log.Debugf("finish init")
	B.acc = acc

	//init and start usershells
	for _, s := range B.UserShells {
		err := s.startUserShell(ctx, acc)
		if err != nil {
			return fmt.Errorf("failed to start usershell [%s]. get error: [%+v]", s.ID, err)
		}
	}
	B.Log.Debugf("finish startusershell")

	//begin cmd gatherings
	err := B.runCmdDefs(ctx)

	B.Log.Debugf("Started usershell pool:[%+v] with err:[%+v]", B, err)

	return err
}

// Stop cleans up all resources
func (B *BashPool) Stop() {

	for _, s := range B.UserShells {
		if s.StdinPipe != nil {
			_, _ = io.WriteString(s.StdinPipe, "exit"+"\n") //exit su
		}
		if s.StdinPipe != nil {
			_, _ = io.WriteString(s.StdinPipe, "exit"+"\n") //exit bash
		}
		//s.Cmd.Process.Kill()
	}

	B.Log.Debugf("stop: before Wait()")
	B.wg.Wait()
	B.Log.Debugf("stop: after Wait()")
}

//Gather -- input interface function. Not use it in this service.
func (B *BashPool) Gather(_ telegraf.Accumulator) error {
	return nil
}

//SetParser -- invoked by output plugin
func (B *BashPool) SetParser(parser parsers.Parser) {
	B.parser = parser
}

//init -- initialised by telegraf framework
func init() {
	inputs.Add("bashpool", func() telegraf.Input {
		return &BashPool{
			OutCh:      make(chan MSGLine, 1),
			UserShells: make(map[string]*UserShell),
			CmdDefs:    make(map[string]*CmdDef),
			wg:         sync.WaitGroup{},
		}
	})
}
