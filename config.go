package main

import (
	"dannytools/ehand"
	"dannytools/logging"
	"dannytools/mydb"
	"database/sql"
	"flag"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/toolkits/file"
	"github.com/toolkits/slice"
)

const (
	//apps/svr/mysql/v5.7.23/bin/mysqld, Version: 5.7.23-log (MySQL Community Server (GPL)). started with:
	//Tcp port: 3307  Unix socket: /tmp/mysql3307.sock
	//Time                 Id Command    Argument
	//2018-10-22T11:43:19.756607+08:00	   38 Query	START TRANSACTION
	//2018-10-22T11:43:19.756799+08:00	   38 Query	update emp set sa = 9999.999 where id = 750
	//2018-10-23T01:28:34.005122Z	132036216 Query	show global variables like 'general_log'
	cGenlogQueryRegexp string = `^\d+-\d+-\d+T\d+:\d+:\d+\.\d+.+\s+\d+\s+(\w+)\s+(.+)`

	cQueryInsert string = "insert"
	cQueryUpdate string = "update"
	cQueryDelete string = "delete"
	cQuerySelect string = "select"
	cQueryDml    string = "dml"

	cQueryDdl      string = "ddl"
	cQueryCreate   string = "create"
	cQueryDrop     string = "drop"
	cQueryAlter    string = "alter"
	cQueryTruncate string = "truncate"
	cQueryRename   string = "rename"
	//cDdlRegexp     string = `^\s*(alter|create|rename|truncate|drop)`

	cQuerySet string = "set"
)

var (
	gConfCmd           *ConfCmd       = &ConfCmd{}
	gLogger            *logging.MyLog = &logging.MyLog{}
	gQueryDmlSupported []string       = []string{
		cQueryDelete,
		cQueryInsert,
		cQueryUpdate,
		cQuerySelect,
	}
	gQueryDdlSupported []string = []string{
		cQueryCreate,
		cQueryDrop,
		cQueryAlter,
		cQueryTruncate,
		cQueryRename,
	}
	gQueryTypesSupported []string = []string{
		cQueryDml,
		cQueryDdl,
	}
)

type ConfCmd struct {
	Threads           uint
	QueryTypes        string
	MyGeneralLogFile  string
	Interval          int64
	RunTimes          int64
	JsonFile          string
	SqlCntPerJsonFile int
	IfInputJsonFile   bool
	MysqlSocket       string
	MyPort            uint
	MyHost            string
	MyUser            string
	MyPassword        string
	Database          string
	Charset           string
	LogLevel          string
	TargetTypes       []string
	QueryRegexp       string

	ifDmlAll bool
	ifDdlAll bool
}

func (this *ConfCmd) Parse() {

	gQueryTypesSupported = append(gQueryTypesSupported, gQueryDmlSupported...)
	gQueryTypesSupported = append(gQueryTypesSupported, gQueryDdlSupported...)

	flag.StringVar(&this.MysqlSocket, "S", "", "mysql socket file")
	flag.StringVar(&this.MyHost, "H", "127.0.0.1", "mysql host. default 127.0.0.1")
	flag.UintVar(&this.MyPort, "P", 3306, "mysql port, default 3306")
	flag.StringVar(&this.MyUser, "u", "root", "mysql user. default root")
	flag.StringVar(&this.MyPassword, "p", "", "mysql password")
	flag.UintVar(&this.Threads, "t", 4, "threads to run")
	flag.Int64Var(&this.RunTimes, "r", 300, "duration to run, in seconds, default 300 seconds")
	flag.StringVar(&this.QueryTypes, "s", "dml", "query types to run, seperated by comma. valid options: "+strings.Join(gQueryTypesSupported, ",")+". default "+cQueryDml)
	flag.Int64Var(&this.Interval, "i", 10, "interval to print TPS/QPS info, in seconds. default 10")
	flag.StringVar(&this.QueryRegexp, "qe", cGenlogQueryRegexp, "regular expression to match one sql in one line general log, the firt submatch must be Command, the second must be Argument")
	flag.StringVar(&this.Database, "d", "", "default database to use when connecting mysql")
	flag.StringVar(&this.Charset, "c", "utf8mb4", "charset to set when connecting mysql, default utf8mb4")
	flag.StringVar(&this.LogLevel, "L", "warning", "log level, valid options are: "+logging.GetAllLogLevelsString(",")+". default warning. ")
	flag.StringVar(&this.JsonFile, "j", "", "if set, parse mysql general log and only output sql into json file, will not apply these sql to mysql")
	flag.IntVar(&this.SqlCntPerJsonFile, "C", 0, "count of sqls in each json file. default 10000, means all sqls are put into one json file")
	flag.BoolVar(&this.IfInputJsonFile, "J", false, "sql source file to apply to mysql is json file. default false, means mysql general log")
	flag.Parse()
	if !logging.CheckLogLevel(this.LogLevel) {
		gLogger.WriteToLogByFieldsExitMsgNoErr("invalid log level "+this.LogLevel, logging.ERROR, ehand.ERR_INVALID_OPTION)
	} else {
		gLogger.ResetLogLevel(this.LogLevel)
	}

	if flag.NArg() != 1 {
		gLogger.WriteToLogByFieldsExitMsgNoErr("mysql general log file or sql file in json format must be set in the end of the command line", logging.ERROR, ehand.ERR_MISSING_OPTION)
	}
	this.MyGeneralLogFile = flag.Arg(0)
	if !file.IsFile(this.MyGeneralLogFile) {
		gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s not exists nor a file", this.MyGeneralLogFile), logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
	}
	if this.JsonFile != "" {
		if !file.IsExist(filepath.Dir(this.JsonFile)) {
			gLogger.WriteToLogByFieldsExitMsgNoErr("dir of "+this.JsonFile+" not exists", logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
		}
	} else {
		if this.MyPassword == "" {
			gLogger.WriteToLogByFieldsExitMsgNoErr("-P password must be set", logging.ERROR, ehand.ERR_FILE_NOT_EXISTS)
		}
	}

	if this.Interval < 1 || this.Interval > 60 {
		gLogger.WriteToLogByFieldsExitMsgNoErr("-i should be between 1 and 60", logging.ERROR, ehand.ERR_INVALID_OPTION)
	}

	//gDdlReg = regexp.MustCompile(this.DdlRegexp)
	gQueryReg = regexp.MustCompile(this.QueryRegexp)

	tmpArr := strings.Split(this.QueryTypes, ",")
	if slice.ContainsString(tmpArr, cQueryDdl) {
		this.ifDdlAll = true
	}
	if slice.ContainsString(tmpArr, cQueryDml) {
		this.ifDmlAll = true
	}

	for i := range tmpArr {
		if !slice.ContainsString(gQueryTypesSupported, tmpArr[i]) {
			gLogger.WriteToLogByFieldsExitMsgNoErr(fmt.Sprintf("%s in -s %s is invalid", tmpArr[i], this.QueryTypes),
				logging.ERROR, ehand.ERR_INVALID_OPTION)
		} else {
			if tmpArr[i] == cQueryDdl || tmpArr[i] == cQueryDml {
				continue
			}
			if slice.ContainsString(gQueryDdlSupported, tmpArr[i]) {
				if this.ifDdlAll {
					continue
				} else {
					this.TargetTypes = append(this.TargetTypes, tmpArr[i])
				}
			}

			if slice.ContainsString(gQueryDmlSupported, tmpArr[i]) {
				if this.ifDmlAll {
					continue
				} else {
					this.TargetTypes = append(this.TargetTypes, tmpArr[i])
				}
			}

		}
	}
	if this.ifDdlAll {
		this.TargetTypes = append(this.TargetTypes, gQueryDdlSupported...)
	}
	if this.ifDmlAll {
		this.TargetTypes = append(this.TargetTypes, gQueryDmlSupported...)
	}

}

func (this *ConfCmd) CreateMySqlConnection() *sql.DB {
	mycfg := mydb.MysqlConCfg{
		User:         this.MyUser,
		Password:     this.MyPassword,
		Timeout:      5,
		WriteTimeout: 5,
		ReadTimeout:  5,
		ParseTime:    false,
		AutoCommit:   true,
		Charset:      this.Charset,
	}
	if this.MysqlSocket != "" {
		mycfg.Socket = this.MysqlSocket
	} else {
		mycfg.Host = this.MyHost
		mycfg.Port = int(this.MyPort)
	}
	if this.Database != "" {
		mycfg.DefaultDb = this.Database
	}

	mycfg.BuildMysqlUrl()

	db, err := mycfg.CreateMysqlConSafe()
	if err != nil {
		gLogger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to connect to mysql", logging.ERROR, ehand.ERR_MYSQL_CONNECTION)
	}
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxIdleConns(3)
	db.SetMaxOpenConns(4 * int(this.Threads))
	return db
}
