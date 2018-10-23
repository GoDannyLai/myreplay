package main

import (
	"bufio"
	"dannytools/constvar"
	"dannytools/ehand"
	"dannytools/logging"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	gQueryReg   *regexp.Regexp = regexp.MustCompile(cGenlogQueryRegexp)
	gConnectReg *regexp.Regexp = regexp.MustCompile(`\s+on\s+(\w+)\s+using`)

	//gDdlReg *regexp.Regexp = regexp.MustCompile(cDdlRegexp)
)

type SqlInfo struct {
	SqlStr  string
	SqlType string
}

type RunningInfo struct {
	SqlType   string
	TimeSpent int64 // nanosecond
	IfErr     bool
}

func ReadMyGeneralLog(cfg *ConfCmd, sqlChan chan *SqlInfo, db *sql.DB) {
	var (
		line      string
		err       error
		fh        *os.File
		bufFh     *bufio.Reader
		ok        bool
		arr       []string
		command   string
		sqlStr    string
		sqlStrAll string
		ifInSql   bool = false
		sqlType   string
	)
	defer close(sqlChan)

	fh, err = os.Open(cfg.MyGeneralLogFile)
	if fh != nil {
		defer fh.Close()
	}
	if err != nil {
		gLogger.WriteToLogByFieldsErrorExtramsgExit(err, "fail to open file "+cfg.MyGeneralLogFile, logging.ERROR, ehand.ERR_FILE_OPEN)
	}
	gLogger.WriteToLogByFieldsNormalOnlyMsg("start thread to reading "+cfg.MyGeneralLogFile, logging.INFO)

	bufFh = bufio.NewReader(fh)
	for {
		line, err = bufFh.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				gLogger.WriteToLogByFieldsNormalOnlyMsg("finish reading "+cfg.MyGeneralLogFile, logging.INFO)
				break
			} else {
				gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to read "+cfg.MyGeneralLogFile, logging.ERROR, ehand.ERR_FILE_READ)
				break
			}
		}

		line = strings.TrimSpace(line)
		/*
			if strings.Contains(line, "mysqld, Version:") || strings.HasSuffix(line, "started with:") {

				continue
			}
			if strings.HasPrefix(line, "Tcp port:") || strings.HasPrefix(line, "Time") {

				continue
			}
		*/

		arr = gQueryReg.FindStringSubmatch(line)
		if len(arr) == 0 {
			if ifInSql {
				sqlStrAll += "\n" + line
			}
			continue
		}
		if ifInSql {
			ifInSql = false
			sqlChan <- &SqlInfo{SqlStr: sqlStrAll, SqlType: sqlType}
			sqlStrAll = ""
		}

		command = arr[1]
		sqlStr = arr[2]
		/*
			2018-10-23T16:38:46.408496+08:00           57 Init DB   danny
			2018-10-23T16:19:29.092785+08:00           58 Connect   danny@xx.xx.xx.xx on danny using TCP/IP
			2018-10-23T16:19:29.093165+08:00           58 Query     SET NAMES 'utf8' COLLATE 'utf8_general_ci'
			2018-10-23T16:19:29.093383+08:00           58 Query     SET @@session.autocommit = ON
			2018-10-23T16:19:29.093894+08:00           58 Query     set session wait_timeout = 3600
			2018-10-23T16:19:29.094132+08:00           58 Query     set session lock_wait_timeout = 60
			2018-10-23T16:19:29.094332+08:00           58 Query     set session innodb_lock_wait_timeout = 30
		*/

		if command != "Query" {
			tmpArr := strings.Fields(sqlStr)
			if command == "Init" {

				if len(tmpArr) >= 2 {
					if tmpArr[0] == "DB" {
						_, err = db.Exec("use " + tmpArr[1])
						if err != nil {
							gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to execute: use "+tmpArr[1], logging.ERROR, ehand.ERR_MYSQL_QUERY)
						}
					}
				}

			} else if command == "Connect" {
				tArr := gConnectReg.FindStringSubmatch(sqlStr)
				if len(tArr) == 2 {
					_, err = db.Exec("use " + tArr[1])
					if err != nil {
						gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "fail to execute: use "+tArr[1], logging.ERROR, ehand.ERR_MYSQL_QUERY)
					}
				}
			}
			continue
		}
		if sqlType, ok = checkIfTargetTypes(sqlStr, cfg); !ok {
			continue
		}
		ifInSql = true
		sqlStrAll = sqlStr

	}
	gLogger.WriteToLogByFieldsNormalOnlyMsg("exit thread to reading "+cfg.MyGeneralLogFile, logging.INFO)
}

func checkIfTargetTypes(sqlStr string, cfg *ConfCmd) (string, bool) {
	/*
		if cfg.IfAllSqlTypes {
			return true
		}
	*/
	sqlStr = strings.ToLower(strings.TrimSpace(sqlStr))
	for i := range cfg.TargetTypes {
		if strings.HasPrefix(sqlStr, cfg.TargetTypes[i]) {
			return cfg.TargetTypes[i], true
		}
	}
	return "", false
}

func StartReplayThreads(cfg *ConfCmd, sqlChan chan *SqlInfo, statsChan chan *RunningInfo, db *sql.DB, wg *sync.WaitGroup) {
	var i uint = 1
	for ; i <= cfg.Threads; i++ {
		wg.Add(1)
		go ReplaySql(i, sqlChan, statsChan, db, wg)
	}
}

func ReplaySql(threadIdx uint, sqlChan chan *SqlInfo, statsChan chan *RunningInfo, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		err      error
		sqlInfo  *SqlInfo
		n        time.Time
		rows     *sql.Rows
		lapsTime int64
		ifErr    bool
	)
	gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("start thread %d to relay sql", threadIdx), logging.INFO)

	for sqlInfo = range sqlChan {
		lapsTime = 0
		if sqlInfo.SqlType == cQuerySelect {
			n = time.Now()
			rows, err = db.Query(sqlInfo.SqlStr)
			lapsTime = time.Since(n).Nanoseconds()
			if err != nil {
				if rows != nil {
					rows.Close()
				}
				gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to execute "+sqlInfo.SqlStr, logging.DEBUG, ehand.ERR_MYSQL_QUERY)
				ifErr = true
			} else {
				for rows.Next() {
					// discard result
				}
				rows.Close()
				ifErr = false
			}
		} else {
			n = time.Now()
			_, err = db.Exec(sqlInfo.SqlStr)
			lapsTime = time.Since(n).Nanoseconds()
			if err != nil {
				gLogger.WriteToLogByFieldsErrorExtramsgExitCode(err, "error to execute "+sqlInfo.SqlStr, logging.DEBUG, ehand.ERR_MYSQL_QUERY)
				ifErr = true
			} else {
				ifErr = false
			}
		}
		statsChan <- &RunningInfo{SqlType: sqlInfo.SqlType, TimeSpent: lapsTime, IfErr: ifErr}

	}

	gLogger.WriteToLogByFieldsNormalOnlyMsg(fmt.Sprintf("exit thread %d to relay sql", threadIdx), logging.INFO)
}

type StatsInfo struct {
	TotalTime       int64 //nanoseconds
	MaxResponseTime int64 //nanoseconds
	MinResponseTime int64 //nanoseconds
	AvgResponseTime int64 //nanoseconds
	TotalSql        int64

	InsertSql int64
	UpdateSql int64
	DeleteSql int64
	SelectSql int64

	DropSql     int64
	AlterSql    int64
	CreateSql   int64
	RenameSql   int64
	TruncateSql int64

	TotalErrSql int64

	InsertErrSql int64
	UpdateErrSql int64
	DeleteErrSql int64
	SelectErrSql int64

	DropErrSql     int64
	AlterErrSql    int64
	CreateErrSql   int64
	RenameErrSql   int64
	TruncateErrSql int64
}

func CalculateStats(statsChan chan *RunningInfo, interval int64, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		sts                  *StatsInfo = &StatsInfo{}
		oneSqlSts            *RunningInfo
		deltaInsert          int64  = 0
		deltaUpdate          int64  = 0
		deltaDelete          int64  = 0
		deltaSelect          int64  = 0
		deltaCreate          int64  = 0
		deltaDrop            int64  = 0
		deltaAlter           int64  = 0
		deltaRename          int64  = 0
		deltaTruncate        int64  = 0
		lastPrintTime        int64  = 0
		deltaTime            int64  = 0
		lineCntToPrintHeader int    = 10
		headerLine           string = fmt.Sprintf("%20s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s %15s", "datetime",
			"drop/s", "create/s", "alter/s", "rename/s", "truncate/s",
			"insert/s", "update/s", "delete/s", "select/s", "TPS", "QPS")
		printLinCnt    int  = -1
		ifGetStartTime bool = false
		startTime      int64
	)
	gLogger.WriteToLogByFieldsNormalOnlyMsg("start stats thread", logging.INFO)

	for oneSqlSts = range statsChan {
		if !ifGetStartTime {
			startTime = time.Now().UnixNano()
			ifGetStartTime = true
		}
		if lastPrintTime == 0 {
			lastPrintTime = time.Now().Unix()
		}
		if oneSqlSts.IfErr {
			sts.TotalErrSql++
		} else {
			sts.TotalSql++
			sts.TotalTime += oneSqlSts.TimeSpent
			if sts.MaxResponseTime < oneSqlSts.TimeSpent {
				sts.MaxResponseTime = oneSqlSts.TimeSpent
			}
			if sts.MinResponseTime == 0 || sts.MinResponseTime > oneSqlSts.TimeSpent {
				sts.MinResponseTime = oneSqlSts.TimeSpent
			}

		}
		if oneSqlSts.SqlType == cQuerySelect {
			if oneSqlSts.IfErr {
				sts.SelectErrSql++
			} else {
				sts.SelectSql++
				deltaSelect++
			}
		} else if oneSqlSts.SqlType == cQueryInsert {
			if oneSqlSts.IfErr {
				sts.InsertErrSql++
			} else {
				sts.InsertSql++
				deltaInsert++

			}
		} else if oneSqlSts.SqlType == cQueryDelete {
			if oneSqlSts.IfErr {
				sts.DeleteErrSql++
			} else {
				sts.DeleteSql++
				deltaDelete++
			}
		} else if oneSqlSts.SqlType == cQueryUpdate {
			if oneSqlSts.IfErr {
				sts.UpdateErrSql++
			} else {
				sts.UpdateSql++
				deltaUpdate++
			}
		} else if oneSqlSts.SqlType == cQueryCreate {
			if oneSqlSts.IfErr {
				sts.CreateErrSql++
			} else {
				sts.CreateSql++
				deltaCreate++
			}
		} else if oneSqlSts.SqlType == cQueryAlter {
			if oneSqlSts.IfErr {
				sts.AlterErrSql++
			} else {
				sts.AlterSql++
				deltaAlter++
			}
		} else if oneSqlSts.SqlType == cQueryDrop {
			if oneSqlSts.IfErr {
				sts.DropErrSql++
			} else {
				sts.DropSql++
				deltaDrop++
			}
		} else if oneSqlSts.SqlType == cQueryRename {
			if oneSqlSts.IfErr {
				sts.RenameErrSql++
			} else {
				sts.RenameSql++
				deltaRename++
			}
		} else if oneSqlSts.SqlType == cQueryTruncate {
			if oneSqlSts.IfErr {
				sts.TruncateErrSql++
			} else {
				sts.TruncateSql++
				deltaTruncate++
			}
		}

		deltaTime = time.Now().Unix() - lastPrintTime
		if deltaTime >= interval {
			if printLinCnt == -1 {
				fmt.Println(headerLine)
				printLinCnt = 0
			} else if printLinCnt >= lineCntToPrintHeader {
				fmt.Println(headerLine)
				printLinCnt = 0
			} else {
				printLinCnt++
			}
			//"drop/s", "create/s", "alter/s", "rename/s", "truncate/s",
			//"insert/s", "update/s", "delete/s", "select/s", "TPS", "QPS"
			/*
				fmt.Printf("%20s %15d %15d %15d %15d %15d %15d %15d %15d %15d %15d %15d\n", time.Unix(lastPrintTime, 0).Format(constvar.DATETIME_FORMAT),
					deltaDrop/deltaTime, deltaCreate/deltaTime, deltaAlter/deltaTime, deltaRename/deltaTime, deltaTruncate/deltaTime,
					deltaInsert/deltaTime, deltaUpdate/deltaTime, deltaDelete/deltaTime, deltaSelect/deltaTime,
					(deltaInsert+deltaUpdate+deltaDelete)/deltaTime, (deltaInsert+deltaUpdate+deltaDelete+deltaSelect)/deltaTime)
			*/
			fmt.Printf("%20s %15.2f %15.2f %15.2f %15.2f %15.2f %15.2f %15.2f %15.2f %15.2f %15.2f %15.2f\n", time.Unix(lastPrintTime, 0).Format(constvar.DATETIME_FORMAT),
				dividInt(deltaDrop, deltaTime), dividInt(deltaCreate, deltaTime), dividInt(deltaAlter, deltaTime), dividInt(deltaRename, deltaTime), dividInt(deltaTruncate, deltaTime),
				dividInt(deltaInsert, deltaTime), dividInt(deltaUpdate, deltaTime), dividInt(deltaDelete, deltaTime), dividInt(deltaSelect, deltaTime),
				dividInt(deltaInsert+deltaUpdate+deltaDelete, deltaTime), dividInt(deltaInsert+deltaUpdate+deltaDelete+deltaSelect, deltaTime))
			deltaInsert = 0
			deltaUpdate = 0
			deltaDelete = 0
			deltaSelect = 0

			deltaDrop = 0
			deltaCreate = 0
			deltaAlter = 0
			deltaRename = 0
			deltaTruncate = 0

			lastPrintTime = time.Now().Unix()
		}
	}
	totalExecTime := dividInt(time.Now().UnixNano()-startTime, 1000*1000*1000) // seconds
	if sts.TotalSql > 0 {
		sts.AvgResponseTime = sts.TotalTime / sts.TotalSql
	} else {
		sts.AvgResponseTime = 0
	}
	totalTps := sts.InsertSql + sts.UpdateSql + sts.DeleteSql
	totalQps := totalTps + sts.SelectSql

	msg := fmt.Sprintf("Summary: \n\tTotal Executed Sqls: %d\n\tTotal Time Taken: %.2fs\n\tSuccessfully Executed:\n\t\t", sts.TotalSql+sts.TotalErrSql, totalExecTime)

	msg += fmt.Sprintf("Total Sqls: %d\n\t\tTotal Response time: %.2fs\n\t\tAvg Response time: %dus\n\t\tMax Response time: %dus\n\t\tMin Response time: %dus\n\t\t",
		sts.TotalSql, dividInt(sts.TotalTime, 1000*1000*1000), sts.AvgResponseTime/1000, sts.MaxResponseTime/1000, sts.MinResponseTime/1000)

	msg += fmt.Sprintf("Total inserts: %d\n\t\tTotal updates: %d\n\t\tTotal deletes: %d\n\t\tTotal selects: %d\n\t\t",
		sts.InsertSql, sts.UpdateSql, sts.DeleteSql, sts.SelectSql)

	msg += fmt.Sprintf("Inserts/s: %.2f\n\t\tUpdates/s: %.2f\n\t\tDeletes/s: %.2f\n\t\tSelects/s: %.2f\n\t\tTps: %.2f\n\t\tQps: %.2f\n\t\t",
		dividIntFloat(sts.InsertSql, totalExecTime), dividIntFloat(sts.UpdateSql, totalExecTime), dividIntFloat(sts.DeleteSql, totalExecTime), dividIntFloat(sts.SelectSql, totalExecTime),
		dividIntFloat(totalTps, totalExecTime), dividIntFloat(totalQps, totalExecTime))

	msg += fmt.Sprintf("Total creates: %d\n\t\tTotal alters: %d\n\t\tTotal drops: %d\n\t\tTotal renames: %d\n\t\tTotal truncates: %d\n\t\t",
		sts.CreateSql, sts.AlterSql, sts.DropSql, sts.RenameSql, sts.TruncateSql)

	msg += fmt.Sprintf("Creates/s: %.2f\n\t\tAlters/s: %.2f\n\t\tDrops/s: %.2f\n\t\tRenames/s: %.2f\n\t\tTruncates/s: %.2f\n\t\t",
		dividIntFloat(sts.CreateSql, totalExecTime), dividIntFloat(sts.AlterSql, totalExecTime), dividIntFloat(sts.DropSql, totalExecTime),
		dividIntFloat(sts.RenameSql, totalExecTime), dividIntFloat(sts.TruncateSql, totalExecTime))

	msg += fmt.Sprintf("\n\tError Executed:\n\t\tTotal Error Sqls: %d\n\t\t", sts.TotalErrSql)
	msg += fmt.Sprintf("Total Error inserts: %d\n\t\tTotal Error updates: %d\n\t\tTotal Error deletes: %d\n\t\tTotal Error selects: %d\n\t\t",
		sts.InsertErrSql, sts.UpdateErrSql, sts.DeleteErrSql, sts.SelectErrSql)
	msg += fmt.Sprintf("Total Error creates: %d\n\t\tTotal Error alters: %d\n\t\tTotal Error drops: %d\n\t\tTotal Error renames: %d\n\t\tTotal Error truncates: %d\n\t\t",
		sts.CreateErrSql, sts.AlterErrSql, sts.DropErrSql, sts.RenameErrSql, sts.TruncateErrSql)

	gLogger.WriteToLogByFieldsNormalOnlyMsg("exit stats thread", logging.INFO)

	fmt.Println(msg)

}

func dividInt(x, y int64) float64 {
	return float64(x) / float64(y)
}

func dividIntFloat(x int64, y float64) float64 {
	return float64(x) / y
}
