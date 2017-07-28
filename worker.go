package main

import (
	"github.com/dzendmitry/logger"
	"fmt"
	"time"
	"math/rand"
)

const (
	JitterTime = 10
	SelectSqlRequestTemplate = "SELECT * FROM %s LIMIT ? OFFSET ?"
)

type workerAnswer struct {
	err error
}

type worker struct {
	c *connector
	r *rand.Rand
	table string
	sqlRequest string
	log logger.ILogger
}

func GetWorker(connector *connector, table string) *worker {
	return &worker {
		c: connector,
		sqlRequest: fmt.Sprintf(SelectSqlRequestTemplate, table),
		table: table,
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
		log: logger.InitConsoleLogger(fmt.Sprintf("WORKER-%v-%v", connector.config.name, table)),
	}
}

func (w *worker) Run() {
	go func() {

		var data tablesInterface
		switch w.table {
		case TableUsers:
			data = new(tableUsers)
		case TableSales:
			data = new(tableSales)
		default:
			w.log.Fatalf("Error unknown table: %v", w.table)
			w.c.writerCmdCh <- writerCmd{}
			return
		}

		var fails int
		var offset int

		L1:
		for {
			err := w.c.db.Ping()
			if err != nil {
				w.log.Warnf("Error while ping: %+v", err)
				if w.checkFails(&fails) {
					return
				} else {
					continue L1
				}
			}
			rows, err := w.c.db.Query(w.sqlRequest, w.c.config.selectLimit, offset)
			if err != nil {
				w.log.Warnf("Error while SELECT query: %+v", err)
				if w.checkFails(&fails) {
					return
				} else {
					continue L1
				}
			}
			csvData := make([]string, 0)
			for rows.Next() {
				if err := rows.Scan(data.unwrapFields()...); err != nil {
					w.log.Warnf("Error scanning data from table %v: %+v", w.table, err)
					if w.checkFails(&fails) {
						return
					} else {
						continue L1
					}
				}
				csvData = append(csvData, data.csvString())
			}
			if err := rows.Err(); err != nil {
				w.log.Warnf("Error in rows for table %v: %+v", w.table, err)
				if w.checkFails(&fails) {
					return
				} else {
					continue L1
				}
			}
			if err := rows.Close(); err != nil {
				w.log.Warnf("Error while closing rows for table %v: %+v", w.table, err)
				if w.checkFails(&fails) {
					return
				} else {
					continue L1
				}
			}
			if len(csvData) == 0 {
				w.c.writerCmdCh <- writerCmd{}
				return
			}
			answerCh := make(chan workerAnswer)
			w.c.writerCmdCh <- writerCmd{
				server: w.c.config.name,
				table: w.table,
				data: csvData,
				answerCh: answerCh,
			}
			answer := <-answerCh
			if answer.err != nil {
				w.log.Warnf("Error writing: %+v", err)
				if w.checkFails(&fails) {
					return
				} else {
					continue L1
				}
			} else {
				fails = 0
				offset += w.c.config.selectLimit
			}
		}
	}()
}

func (w *worker) checkFails(fails *int) bool {
	*fails += 1
	if *fails >= w.c.config.retry {
		w.c.writerCmdCh <- writerCmd{}
		w.log.Fatalf("Error fails have reached the number of attempts")
		return true
	} else {
		time.Sleep(time.Duration(w.c.config.timeout + (w.r.Intn(JitterTime) - JitterTime / 2)) * time.Millisecond)
		return false
	}
}