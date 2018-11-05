package main

import (
	"sync"
)

func main() {
	gLogger.CreateNewRawLogger()
	gConfCmd.Parse()
	if gConfCmd.JsonFile != "" {
		// just parse general log and output sqls into json file
		ReadMyGeneralLogAndGenJson(gConfCmd)

	} else {
		dbCon := gConfCmd.CreateMySqlConnection()

		var (
			sqlChan   chan *SqlInfo     = make(chan *SqlInfo, 2*gConfCmd.Threads)
			statsChan chan *RunningInfo = make(chan *RunningInfo, 4*gConfCmd.Threads)
			sqlWait   sync.WaitGroup    = sync.WaitGroup{}
			stsWait   sync.WaitGroup    = sync.WaitGroup{}
		)

		StartReplayThreads(gConfCmd, sqlChan, statsChan, dbCon, &sqlWait)

		if gConfCmd.IfInputJsonFile {
			go ReadFromJsonFile(gConfCmd, sqlChan)
		} else {
			go ReadMyGeneralLog(gConfCmd, sqlChan, dbCon)
		}

		stsWait.Add(1)
		go CalculateStats(statsChan, gConfCmd.Interval, &stsWait)

		sqlWait.Wait()
		close(statsChan)

		stsWait.Wait()
	}
}
