package oraquery

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	_ "github.com/godror/godror"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
)

const sampleConfig = `
### all "name" mean unique ids

  max_dbconns = 5
  reconn_interval_second = 30

[[inputs.oraquery.dsn_defines]]
  name = "yy1"    #unique id
  dsn = "dbtest/dbtest@mach1yy"
[[inputs.oraquery.dsn_defines]]
  name = "xx1"    #unique id
  dsn = "dbtest/dbtest@xx"
[[inputs.oraquery.dsn_defines]]
  name = "yy2"    #unique id
  dsn = "dbtest/dbtest@mach2yy"


### simply config rac sql and ha or single sql to a same metric
### if you are not sure whether the db is a rac.
### and this plugin will judge type of db 
### and select proper type sql to run.
[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "nonrac_dbtime"
  interval_second = 60
  tags = ["tag_id=xyz001", "owner=cheng"] 
[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "rac_dbtime"
  interval_second = 60
  tags = ["tag_id=xyz001", "owner=cheng"] 

[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "nonrac_performance"
  interval_second = 60
  tags = ["tag_id=xyz002", "owner=cheng"] 
[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "rac_performance"
  interval_second = 60
  tags = ["tag_id=xyz002", "owner=cheng"] 

[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "nonrac_topevent5"
  interval_second = 5
  tags = ["tag_id=xyz003", "owner=cheng"] 
[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "rac_topevent5"
  interval_second = 5
  tags = ["tag_id=xyz003", "owner=cheng"] 

[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "tablespace"
  interval_second = 10
  tags = ["tag_id=xyz004", "owner=cheng"] 

[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "nonrac_topsql5"
  interval_second = 100
  tags = ["tag_id=xyz005", "owner=cheng"] 
[[inputs.oraquery.metrics]]
  dsn_name = "xx1"
  sql_name = "rac_topsql5"
  interval_second = 100
  tags = ["tag_id=xyz005", "owner=cheng"] 



[[inputs.oraquery.sql_defines]]
  name = "nonrac_dbtime"
  ### dbtype --- the type of db the sql applys to run on
  ### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
  dbtype = 2
  sqlstr = '''
select sum(value/1000) as db_time_seconds
from V$SESS_TIME_MODEL
where STAT_NAME='DB time'
  '''
  measure_name = "dbtime"
[[inputs.oraquery.sql_defines.tags]]
  ### tags has name, type and column_pos_sql same as fields
  ### but type of tags must be string
  ### column_pos_sql -- the position in sql returning fields, index from 1
  [[inputs.oraquery.sql_defines.fields]]
  name = "db_time_seconds"
  type = "float"
  column_pos_sql = 1


[[inputs.oraquery.sql_defines]]
  name = "rac_dbtime"
  ### dbtype --- the type of db the sql applys to run on
  ### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
  dbtype = 1
  sqlstr = '''
select sum(value/1000) as db_time_seconds
from GV$SESS_TIME_MODEL
where STAT_NAME='DB time'
  '''
  measure_name = "dbtime"
[[inputs.oraquery.sql_defines.tags]]
  ### type of tags must be string
[[inputs.oraquery.sql_defines.fields]]
  name = "db_time_seconds"
  type = "float"
  column_pos_sql = 1


[[inputs.oraquery.sql_defines]]
  name = "nonrac_performance"
  ### dbtype --- the type of db the sql applys to run on
  ### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
  dbtype = 2
  sqlstr = '''
select ta.*, tb.*
from (
	select
	'running' as dbstate,
	(select count(*) from v$session where status='ACTIVE') as sess,
	(select count(*) from v$process) as process,
	(select sum(value) FROM v$sysstat WHERE name='user commits') as usercommits,
	dual.rowid as id
  from dual
  ) ta,
  (
  select aa.*, dual.rowid as id from
  (select *
  from (select WAIT_CLASS, avg(VALUE) as val
			from  (SELECT n.wait_class as WAIT_CLASS,
					round(m.time_waited/m.INTSIZE_CSEC,3) as VALUE
				  FROM v$waitclassmetric m, v$system_wait_class n
				  WHERE m.wait_class_id=n.wait_class_id
					AND n.wait_class != 'Idle')
				  group by WAIT_CLASS) a
	PIVOT(
		MAX(val)
		FOR wait_class
		IN (
			'Application',
			'Network',
			'User I/O',
			'System I/O',
			'Concurrency',
			'Commit'
		)
	)) aa, dual
  ) tb
  where ta.id=tb.id
'''
  measure_name = "performance"
[[inputs.oraquery.sql_defines.tags]]
### type of tags must be string
[[inputs.oraquery.sql_defines.fields]]
  name = "dbstate"
  type = "string"
  column_pos_sql = 1
[[inputs.oraquery.sql_defines.fields]]
  name = "session"
  type = "int"
  column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
  name = "process"
  type = "int"
  column_pos_sql = 3
[[inputs.oraquery.sql_defines.fields]]
  name = "usercommits"
  type = "int"
  column_pos_sql = 4
[[inputs.oraquery.sql_defines.fields]]
  name = "application"
  type = "float"
  column_pos_sql = 6
[[inputs.oraquery.sql_defines.fields]]
  name = "network"
  type = "float"
  column_pos_sql = 7
[[inputs.oraquery.sql_defines.fields]]
  name = "userIO"
  type = "float"
  column_pos_sql = 8
[[inputs.oraquery.sql_defines.fields]]
  name = "systemIO"
  type = "float"
  column_pos_sql = 9
[[inputs.oraquery.sql_defines.fields]]
  name = "concurrency"
  type = "float"
  column_pos_sql = 10
[[inputs.oraquery.sql_defines.fields]]
  name = "commit"
  type = "float"
  column_pos_sql = 11
  
  
[[inputs.oraquery.sql_defines]]
  name = "rac_performance"
  ### dbtype --- the type of db the sql applys to run on
  ### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
  dbtype = 1
  sqlstr = '''
select ta.*, tb.*
from (
select
'running' as dbstate,
(select count(*) from gv$session where status='ACTIVE') as sess,
(select count(*) from gv$process) as process,
(select sum(value) FROM gv$sysstat WHERE name='user commits') as usercommits,
dual.rowid as id
from dual
) ta,
(
select aa.*, dual.rowid as id from
(select *
from (select WAIT_CLASS, avg(VALUE) as val
			from  (SELECT n.wait_class as WAIT_CLASS,
					round(m.time_waited/m.INTSIZE_CSEC,3) as VALUE
				FROM gv$waitclassmetric m, gv$system_wait_class n
				WHERE m.wait_class_id=n.wait_class_id
					AND n.wait_class != 'Idle')
				group by WAIT_CLASS) a
	PIVOT(
		MAX(val)
		FOR wait_class
		IN (
			'Application',
			'Network',
			'User I/O',
			'System I/O',
			'Concurrency',
			'Commit'
		)
	)) aa, dual
) tb
where ta.id=tb.id
	'''
  measure_name = "performance"
[[inputs.oraquery.sql_defines.tags]]
	### type of tags must be string
[[inputs.oraquery.sql_defines.fields]]
	name = "dbstate"
	type = "string"
	column_pos_sql = 1
	[[inputs.oraquery.sql_defines.fields]]
	name = "session"
	type = "int"
	column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
	name = "process"
	type = "int"
	column_pos_sql = 3
[[inputs.oraquery.sql_defines.fields]]
	name = "usercommits"
	type = "int"
	column_pos_sql = 4
[[inputs.oraquery.sql_defines.fields]]
	name = "application"
	type = "float"
	column_pos_sql = 6
[[inputs.oraquery.sql_defines.fields]]
	name = "network"
	type = "float"
	column_pos_sql = 7
[[inputs.oraquery.sql_defines.fields]]
	name = "userIO"
	type = "float"
	column_pos_sql = 8
[[inputs.oraquery.sql_defines.fields]]
	name = "systemIO"
	type = "float"
	column_pos_sql = 9
[[inputs.oraquery.sql_defines.fields]]
	name = "concurrency"
	type = "float"
	column_pos_sql = 10
	[[inputs.oraquery.sql_defines.fields]]
	name = "commit"
	type = "float"
	column_pos_sql = 11
	
	
[[inputs.oraquery.sql_defines]]
	name = "nonrac_topevent5"
	### dbtype --- the type of db the sql applys to run on
	### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
	dbtype = 2
	sqlstr = '''
SELECT *
FROM (SELECT EVENT,
		WAIT_CLASS,
		100*round(count(*) / sum(count(*)) over(), 2) PCT
		FROM V$ACTIVE_SESSION_HISTORY
		WHERE TIME_WAITED > 0
		AND sample_time > sysdate - 1
		GROUP BY EVENT,WAIT_CLASS)
WHERE ROWNUM <= 5
ORDER BY PCT desc
	'''
	measure_name = "topevent"
[[inputs.oraquery.sql_defines.tags]]
	### type of tags must be string
[[inputs.oraquery.sql_defines.fields]]
	name = "event"
	type = "string"
	column_pos_sql = 1
	[[inputs.oraquery.sql_defines.fields]]
	name = "wait_class"
	type = "string"
	column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
	name = "pct"
	type = "int"
	column_pos_sql = 3
	
	
[[inputs.oraquery.sql_defines]]
	name = "rac_topevent5"
	### dbtype --- the type of db the sql applys to run on
	### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
	dbtype = 1
	sqlstr = '''
SELECT *
FROM (SELECT EVENT,
		WAIT_CLASS,
		100*round(count(*) / sum(count(*)) over(), 2) PCT
		FROM GV$ACTIVE_SESSION_HISTORY
		WHERE TIME_WAITED > 0
		AND sample_time > sysdate - 1
		GROUP BY EVENT,WAIT_CLASS)
WHERE ROWNUM <= 5
ORDER BY PCT desc
	'''
	measure_name = "topevent"
[[inputs.oraquery.sql_defines.tags]]
	### type of tags must be string
	[[inputs.oraquery.sql_defines.fields]]
	name = "event"
	type = "string"
	column_pos_sql = 1
[[inputs.oraquery.sql_defines.fields]]
	name = "wait_class"
	type = "string"
	column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
	name = "pct"
	type = "int"
	column_pos_sql = 3


[[inputs.oraquery.sql_defines]]
	name = "tablespace"
	### dbtype --- the type of db the sql applys to run on
	### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
	dbtype = 0
	sqlstr = '''
SELECT Upper(F.TABLESPACE_NAME)         "TBS_NAME",
		D.TOT_GROOTTE_MB                 "total_MB",
		F.TOTAL_BYTES                    "free_MB"
FROM   (SELECT TABLESPACE_NAME,
				Round(Sum(BYTES) / ( 1024 * 1024 ), 2) TOTAL_BYTES,
				Round(Max(BYTES) / ( 1024 * 1024 ), 2) MAX_BYTES
		FROM   SYS.DBA_FREE_SPACE
		GROUP  BY TABLESPACE_NAME) F,
		(SELECT DD.TABLESPACE_NAME,
				Round(Sum(DD.BYTES) / ( 1024 * 1024 ), 2) TOT_GROOTTE_MB
		FROM   SYS.DBA_DATA_FILES DD
		GROUP  BY DD.TABLESPACE_NAME) D
WHERE  D.TABLESPACE_NAME = F.TABLESPACE_NAME
union
SELECT a.tablespace_name TBS_NAME,
		a.BYTES/1024/1024 total_MB,
		(a.bytes - nvl(b.bytes, 0))/1024/1024 free_MB
FROM (SELECT   tablespace_name,
				SUM (bytes) bytes
		FROM dba_temp_files GROUP BY tablespace_name) a,
			(SELECT   tablespace_name,
				SUM (bytes_cached) bytes
			FROM v$temp_extent_pool GROUP BY tablespace_name) b
WHERE a.tablespace_name = b.tablespace_name
	'''
	measure_name = "tablespace"
[[inputs.oraquery.sql_defines.tags]]
	### type of tags must be string
	####column_pos_sql = 1   ### the column position of sql result. begin with 1.
[[inputs.oraquery.sql_defines.fields]]
	name = "tbs_name"
	type = "string"
	column_pos_sql = 1
[[inputs.oraquery.sql_defines.fields]]
	name = "total_mb"
	type = "float"
	column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
	name = "free_mb"
	type = "float"
	column_pos_sql = 3
	
	
[[inputs.oraquery.sql_defines]]
	name = "rac_topsql5"
	### dbtype --- the type of db the sql applys to run on
	### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
	dbtype = 1
	sqlstr = '''
	SELECT * FROM( select a.sql_id,a.event,b.sql_text,a.Activity
	from (SELECT sql_id, 100*round(count(*) / sum(count(*)) over(), 2) Activity,
	session_type Event FROM GV$ACTIVE_SESSION_HISTORY  WHERE sample_time > sysdate - 1
	AND session_type <> 'BACKGROUND'  GROUP BY sql_id,session_type) a, gv$sql b
	WHERE a.sql_id = b.sql_id)  WHERE ROWNUM <= 5  ORDER BY Activity desc
	'''
	measure_name = "topsqlinfo"
[[inputs.oraquery.sql_defines.tags]]
	### type of tags must be string
	####name = "SQL_ID"
	####type = "string"
	####column_pos_sql = 1   ### the column position of sql result. begin with 1.
[[inputs.oraquery.sql_defines.fields]]
	name = "SQL_ID"
	type = "string"
	column_pos_sql = 1
[[inputs.oraquery.sql_defines.fields]]
	name = "EVENT"
	type = "string"
	column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
	name = "SQL_TEXT"
	type = "string"
	column_pos_sql = 3
[[inputs.oraquery.sql_defines.fields]]
	name = "ACTIVITY"
	type = "int"
	column_pos_sql = 4


[[inputs.oraquery.sql_defines]]
	name = "nonrac_topsql5"
	### dbtype --- the type of db the sql applys to run on
	### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
	dbtype = 2
	sqlstr = '''
SELECT * FROM( select a.sql_id,a.event,b.sql_text,a.Activity
from (SELECT sql_id, 100*round(count(*) / sum(count(*)) over(), 2) Activity,
session_type Event FROM V$ACTIVE_SESSION_HISTORY  WHERE sample_time > sysdate - 1
AND session_type <> 'BACKGROUND'  GROUP BY sql_id,session_type) a, v$sql b
WHERE a.sql_id = b.sql_id)  WHERE ROWNUM <= 5  ORDER BY Activity desc
'''
	measure_name = "topsqlinfo"
[[inputs.oraquery.sql_defines.fields]]
	name = "SQL_ID"
	type = "string"
	column_pos_sql = 1
[[inputs.oraquery.sql_defines.fields]]
	name = "EVENT"
	type = "string"
	column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
	name = "SQL_TEXT"
	type = "string"
	column_pos_sql = 3
[[inputs.oraquery.sql_defines.fields]]
	name = "ACTIVITY"
	type = "int"
	column_pos_sql = 4


[[inputs.oraquery.sql_defines]]
  name = "dbinfo"
  ### dbtype --- the type of db the sql applys to run on
  ### 1 - "RAC", 2 - "HA or SINGLE", 0 - any type
  dbtype = 0
  sqlstr = '''
select
(select value from v$nls_parameters where PARAMETER='NLS_CHARACTERSET') as database_character_set,
(select value from v$nls_parameters where PARAMETER='NLS_NCHAR_CHARACTERSET') as nation_character_set,
(select value/1024/1024||'M' from v$parameter where name='sga_target') as sga,
(select value/1024/1024||'M' from v$pgastat where name='aggregate PGA target parameter') as pga,
(select value from v$parameter where name='db_files') as db_file,
(select value from v$parameter where name='processes') as process,
(select decode(value, 'TRUE', 'open', 'close') from v$parameter where name = 'log_archive_start') as classifystate,
(select substr(banner_full, instr(banner_full, 'Version ')+8) from v$version) as version,
(select count(*) from v$session where status='ACTIVE') as sss
from dual
'''
  measure_name = "dbinfo"
[[inputs.oraquery.sql_defines.tags]]
[[inputs.oraquery.sql_defines.fields]]
  name = "database_character_set"
  type = "string"
  column_pos_sql = 1
[[inputs.oraquery.sql_defines.fields]]
  name = "nation_character_set"
  type = "string"
  column_pos_sql = 2
[[inputs.oraquery.sql_defines.fields]]
  name = "sga"
  type = "string"
  column_pos_sql = 3
[[inputs.oraquery.sql_defines.fields]]
  name = "pga"
  type = "string"
  column_pos_sql = 4
[[inputs.oraquery.sql_defines.fields]]
  name = "db_file"
  type = "string"
  column_pos_sql = 5
[[inputs.oraquery.sql_defines.fields]]
  name = "process"
  type = "string"
  column_pos_sql = 6
[[inputs.oraquery.sql_defines.fields]]
  name = "classifystate"
  type = "string"
  column_pos_sql = 7
[[inputs.oraquery.sql_defines.fields]]
  name = "version"
  type = "string"
  column_pos_sql = 8
[[inputs.oraquery.sql_defines.fields]]
  name = "session"
  type = "int"
  column_pos_sql = 9
`

//SampleConfig -- input interface function
func (O *OraQuery) SampleConfig() string {
	return sampleConfig
}

//Description -- input interface function
func (O *OraQuery) Description() string {
	return `
#oraquery -- a service to execute oracle sql and get results 
#  which connects oracle db by tns
#  it works on oracle client instant.
#  after installing client instant, before you set the dsn_list
#  please config tnsnames in file $(cat /etc/ld.so.conf.d/oracle-instantclient.conf)/network/admin/tnsnames.ora
`
}

//ItemDef - define fields and tags with type for flush to influxdb
type ItemDef struct {
	Name      string `toml:"name"`
	Type      string `toml:"type"`
	ColumnPos int    `toml:"column_pos_sql"`
}

type SQLDefine struct {
	Name        string    `toml:"name"`
	DBType      int8      `toml:"dbtype"` //1 -- rac, 2 -- non-rac, 0 -- any type
	SQLStr      string    `toml:"sqlstr"`
	MeasureName string    `toml:"measure_name"`
	Tags        []ItemDef `toml:"tags"`
	Fields      []ItemDef `toml:"fields"`
}

type DSNDefine struct {
	Name string `toml:"name"`
	DSN  string `toml:"dsn"`
}

//SQLMetric --
type SQLMetric struct {
	DSNName     string   `toml:"dsn_name"`
	SQLName     string   `toml:"sql_name"`
	IntervalSec int      `toml:"interval_second"`
	Tags        []string `toml:"tags"`
}

//STMT --
type STMT struct {
	stmt      *sql.Stmt
	sqlMetric *SQLMetric
	sqlDefine *SQLDefine
	parent    *ORADB
	stop      chan bool

	lencols   int
	vals      []interface{}
	pvals     []*interface{}
	valsReady bool
}

//ORADB --
type ORADB struct {
	DSN            string
	DB             *sql.DB
	dbname         string
	connected      bool
	stmtsPrepaired bool
	reload         chan bool

	IsRAC     int8
	StmtMap   map[string]*STMT //MeasureName : stmt
	startStep float64
	parent    *OraQuery
	//pingTicker time.Ticker
}

// OraQuery is an input plugin that collects external metrics sent via HTTP
type OraQuery struct {
	MaxOpenConns int          `toml:"max_dbconns"`
	SQLMetrics   []*SQLMetric `toml:"metrics"`
	DSNDefines   []*DSNDefine `toml:"dsn_defines"`
	SQLDefines   []*SQLDefine `toml:"sql_defines"`

	//db connection string
	//DSNList           []string          `toml:"dsn_list"`
	dsnMap            map[string]string
	sqlMap            map[string]*SQLDefine
	oradbs            map[string]*ORADB //key=dsn
	ReconnIntervalSec int               `toml:"reconn_interval_second"`

	//TimeFunc
	Log telegraf.Logger

	wg sync.WaitGroup

	acc telegraf.Accumulator
}

func (s *ORADB) _GetDBNameAndRAC(ctx context.Context) error {
	var (
		dbname, dbtype string
		rac            int8
		err            error
		row            *sql.Row
	)

	err = s.DB.PingContext(ctx)
	if err != nil {
		return err
	}

	row = s.DB.QueryRowContext(ctx, "select ora_database_name from dual")
	err = row.Scan(&dbname)
	if err != nil {
		s.parent.Log.Error("when got dbname:", err)
		return err
	}
	s.dbname = dbname

	row = s.DB.QueryRowContext(ctx, "select database_type from v$instance")
	err = row.Scan(&dbtype)
	if err != nil {
		s.parent.Log.Error("when got database_type:", err)
		return err
	}

	if dbtype == "RAC" {
		rac = 1
	} else {
		rac = 2
	}
	s.IsRAC = rac

	return nil
}

func (s *ORADB) _SetStmts(ctx context.Context) {
	var (
		err            error
		stmt           *sql.Stmt
		numFail        int
		numStmt        int
		minIntervalSec int
	)

	defer func() {
		r := recover()
		if r != nil {
			s.parent.Log.Errorf("_SetStmts got a panic: %v", r)
		}

		if numFail == numStmt || r != nil {
			s.stmtsPrepaired = false
		} else {
			s.stmtsPrepaired = true
		}
	}()

	numFail = 0
	numStmt = 0
	for _, m := range s.parent.SQLMetrics {
		dstr, ok := s.parent.dsnMap[m.DSNName]
		if !ok {
			continue
		}

		if dstr != s.DSN {
			continue
		}

		sd, ok := s.parent.sqlMap[m.SQLName]
		if !ok {
			continue
		}

		if sd.DBType != 0 && sd.DBType != s.IsRAC {
			continue
		}

		if m.IntervalSec <= 0 { //default intervalsec is 10.
			m.IntervalSec = 10
		}

		if minIntervalSec <= 0 || minIntervalSec > m.IntervalSec {
			minIntervalSec = m.IntervalSec
		}

		numStmt++
		stmt, err = s.DB.PrepareContext(ctx, sd.SQLStr)
		if err != nil {
			numFail++
			s.parent.Log.Error("PrepareContext error:", err)
			continue
		}
		s.StmtMap[sd.Name] = &STMT{stmt: stmt,
			parent:    s,
			sqlMetric: m,
			sqlDefine: sd,
			valsReady: false,
			stop:      make(chan bool, 1),
		}
	}

	if numStmt > 0 {
		s.startStep = float64(minIntervalSec) / float64(numStmt)
	} else {
		s.startStep = 0
	}
}

func (s *ORADB) _Reconn(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.connected = false
			s.parent.Log.Errorf("Reconn error, got a panic: %v", r)
		}
	}()

	var err error

	if s.DB != nil && s.DB.PingContext(ctx) == nil && s.connected {
		return
	}

	s.connected = false
	s.DB, err = sql.Open("godror", s.DSN)
	if err != nil {
		s.parent.Log.Error(err)
		return
	}
	s.DB.SetMaxOpenConns(s.parent.MaxOpenConns)

	err = s._GetDBNameAndRAC(ctx)
	if err != nil {
		s.parent.Log.Error("failed to connect to ", s.DSN)
	} else {
		s.connected = true
		s.parent.Log.Info(s.dbname, " connected.")
	}
}

func _SetType(p *interface{}, t string) {
	if strings.Index(strings.ToUpper(t), "INT") == 0 {
		*p = new(int64)
	} else if strings.Index(strings.ToUpper(t), "FLOAT") == 0 {
		*p = new(float64)
	} else if strings.Index(strings.ToUpper(t), "STR") == 0 {
		*p = new(string)
	} else {
		*p = new(sql.RawBytes)
	}
}

func (s *STMT) _SetScanType(icol int, p *interface{}) {
	notset := true

	for _, f := range s.sqlDefine.Fields {
		if icol == (f.ColumnPos - 1) {
			_SetType(p, f.Type)
			notset = false
			break
		}
	}

	//tags must be string
	for _, t := range s.sqlDefine.Tags {
		if icol == (t.ColumnPos - 1) {
			_SetType(p, "string")
			notset = false
			break
		}
	}

	//non-output fields should set to rawbytes
	if notset {
		_SetType(p, "rawbytes")
	}
}

func (s *STMT) _Gather(ctx context.Context) error {
	defer func() {

		if r := recover(); r != nil {
			s.parent.parent.Log.Errorf("_Gather got a panic: %v", r)
		}
	}()

	err := s.parent.DB.PingContext(ctx)
	if err != nil {
		s.parent.parent.Log.Errorf("Connection to %s has a problem before query %s",
			s.parent.dbname, s.sqlDefine.Name)
		return nil
	}

	rows, err := s.stmt.QueryContext(ctx)
	if err != nil {
		s.parent.parent.Log.Errorf("Connection to %s has a problem when query %s",
			s.parent.dbname, s.sqlDefine.Name)
		return nil
	}

	cols, err := rows.Columns()
	if err != nil {
		s.parent.parent.Log.Error(err)
		return err
	}

	if !s.valsReady {
		s.lencols = len(cols)
		s.vals = make([]interface{}, s.lencols)
		s.pvals = make([]*interface{}, s.lencols)
		for i := 0; i < s.lencols; i++ {
			s.pvals[i] = &(s.vals[i])
		}
		for icol, pval := range s.pvals {
			s._SetScanType(icol, pval)
		}
		s.valsReady = true
	}

	for rows.Next() {
		err = rows.Scan(s.vals...)
		if err != nil {
			s.parent.parent.Log.Error(err)
			continue
		}
		//fmt.Println("vals=", s.vals)

		tags := make(map[string]string)
		tags["dbname"] = s.parent.dbname
		cltype := "RAC"
		if s.parent.IsRAC != 1 {
			cltype = "HA"
		}
		tags["clustertype"] = cltype

		for _, t := range s.sqlMetric.Tags {
			kv := strings.Split(t, "=")
			if len(kv) > 1 {
				tags[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}

		fields := make(map[string]interface{})
		for _, f := range s.sqlDefine.Fields {
			//fmt.Println("f=", f)
			idx := f.ColumnPos - 1
			if idx >= 0 && idx < s.lencols {
				if strings.Index(strings.ToUpper(f.Type), "INT") == 0 {
					fields[f.Name] = *(s.vals[idx].(*int64))
				} else if strings.Index(strings.ToUpper(f.Type), "FLOAT") == 0 {
					fields[f.Name] = *(s.vals[idx].(*float64))
				} else {
					fields[f.Name] = *(s.vals[idx].(*string))
				}
			}
		}

		s.parent.parent.acc.AddFields(s.sqlDefine.MeasureName, fields, tags)
	}

	return nil
}

func (s *STMT) _GatherOnce(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(s.sqlMetric.IntervalSec) * time.Second)
	defer ticker.Stop()

	done := make(chan error)
	go func() {
		done <- s._Gather(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			//fmt.Println("_GatherOnce ctx get done message.")
			return nil
		case err := <-done:
			//fmt.Println("done:", err)
			return err
		case <-ticker.C:
			s.parent.parent.Log.Warnf("[%s] did not complete within its interval",
				s.sqlDefine.Name)

		}
	}
}

func (s *STMT) _GatherOnInterval(ctx context.Context) {
	interval := time.Duration(s.sqlMetric.IntervalSec) * time.Second
	//jitter := interval / 2
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		err := s._GatherOnce(ctx)
		if err != nil {
			s.parent.parent.acc.AddError(err)
		}

		select {
		case <-ticker.C:
			continue
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		}
	}
}

func (s *ORADB) runOnDB(ctx context.Context, ith int) {
	wg := new(sync.WaitGroup)
	for <-s.reload {

		if s.DB != nil {
			for _, st := range s.StmtMap {
				st.stmt.Close()
				st.stop <- true
			}
			s.DB.Close()
		}

		//conn db, and set stmts
		s._Reconn(ctx)
		if s.connected {
			s._SetStmts(ctx)
		}

		if s.stmtsPrepaired {
			j := 0.0
			for _, st := range s.StmtMap {
				d := time.Duration((float64(ith*2) +
					j*s.startStep) * float64(time.Second))

				s.parent.wg.Add(1)
				go func(st *STMT, d time.Duration) {
					defer s.parent.wg.Done()
					err := internal.SleepContext(ctx, d)
					if err != nil {
						return
					}

					st._GatherOnInterval(ctx)
				}(st, d)

				j++
			}
		} else {
			_ = internal.SleepContext(ctx,
				time.Duration(s.parent.ReconnIntervalSec)*time.Second)

			s.reload <- true
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case <-ctx.Done():
				s.reload <- false
			case <-s.reload:
				s.reload <- true //lauch a reload
			}
		}()

		wg.Wait()

	}
}

//queryDBs --
func (O *OraQuery) runQuerys(ctx context.Context) error {
	i := 0
	for k := range O.oradbs {
		oradb := O.oradbs[k]

		go func(context.Context, *ORADB, int) {
			oradb.runOnDB(ctx, i)
		}(ctx, oradb, i)

		i++
	}

	return nil
}

//Init -- invoked by telegraf framework before Start
func (O *OraQuery) Init() error {
	return nil
}

// StartContext -- start the service. invoked by telegraf framwork
func (O *OraQuery) StartContext(ctx context.Context, acc telegraf.Accumulator) error {
	/*
		for _, m := range O.SQLMetrics {
			O.Log.Info(m)
		}
	*/
	O.acc = acc

	for _, dd := range O.DSNDefines {
		O.dsnMap[dd.Name] = strings.TrimSpace(dd.DSN)
	}

	for _, sd := range O.SQLDefines {
		O.sqlMap[sd.Name] = sd
	}

	for _, dsn := range O.dsnMap {
		O.oradbs[dsn] = &ORADB{
			DSN:            dsn,
			StmtMap:        make(map[string]*STMT),
			parent:         O,
			reload:         make(chan bool, 1),
			connected:      false,
			stmtsPrepaired: false,
		}
		O.oradbs[dsn].reload <- true
	}
	O.Log.Info("Starting service...")

	return O.runQuerys(ctx)
}

//Gather -- input interface function. Not use it in this service.
func (O *OraQuery) Gather(_ telegraf.Accumulator) error {
	return nil
}

// Stop cleans up all resources
func (O *OraQuery) Stop() {
	defer func() {
		for _, oradb := range O.oradbs {
			//			fmt.Println("oradb", oradb)
			for _, stmt := range oradb.StmtMap {
				if stmt.stmt != nil {
					stmt.stmt.Close()
				}
			}

			if oradb.DB != nil {
				oradb.DB.Close()
			}
		}
	}()

	O.wg.Wait()
}

//init -- initialised by telegraf framework
func init() {
	inputs.Add("oraquery", func() telegraf.Input {
		return &OraQuery{
			oradbs:            make(map[string]*ORADB),
			dsnMap:            make(map[string]string),
			sqlMap:            make(map[string]*SQLDefine),
			MaxOpenConns:      2,
			ReconnIntervalSec: 60,
		}
	})
}
