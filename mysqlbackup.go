package main

import (
	"github.com/dzendmitry/logger"
	"fmt"
)

type config struct {
	name         string
	server       string
	tables       []string
	selectLimit  int
	retry        int
	timeout      int // in ms
	connPoolSize int
}

const (
	TableUsers = "users"
	TableSales = "sales"
)

type tablesInterface interface {
	unwrapFields() []interface{} // можно поиграться с reflect
	csvString() string
}

type tableUsers struct {
	User_id int
	Name string
}

func (t *tableUsers) unwrapFields() []interface{} {
	return []interface{}{
		&(t.User_id),
		&(t.Name),
	}
}

func (t *tableUsers) csvString() string {
	return fmt.Sprintf("%d,%s\n", t.User_id, t.Name)
}

type tableSales struct {
	Order_id int
	User_id int
	Order_amount float64
}

func (t *tableSales) unwrapFields() []interface{} {
	return []interface{}{
		&(t.Order_id),
		&(t.User_id),
		&(t.Order_amount),
	}
}

func (t *tableSales) csvString() string {
	return fmt.Sprintf("%d,%d,%.2f\n", t.Order_id, t.User_id, t.Order_amount)
}

var configList = []config{
	config{
		name: "thailand",
		server: "root:password@tcp(localhost:3307)/db_for_backup",
		tables: []string{TableUsers, TableSales},
		selectLimit: 100,
		retry: 3,
		timeout: 500,
		connPoolSize: 10,
	},
	config{
		name: "indonesia",
		server: "root:password@tcp(localhost:3308)/db_for_backup",
		tables: []string{TableUsers, TableSales},
		selectLimit: 100,
		retry: 3,
		timeout: 500,
		connPoolSize: 10,
	},
	config{
		name: "laos",
		server: "root:password@tcp(localhost:3309)/db_for_backup",
		tables: []string{TableUsers, TableSales},
		selectLimit: 100,
		retry: 3,
		timeout: 500,
		connPoolSize: 10,
	},
}

func main() {
	log := logger.InitConsoleLogger("MYSQLDUMP")
	defer log.Close()

	if len(configList) == 0 {
		log.Infof("There are no configs in configList")
		return
	}

	writer := NewWriter()
	defer writer.Stop()

	var workersCount int
	for _, config := range configList {
		c, err := NewConnector(config, writer.cmdCh)
		if err != nil {
			log.Fatalf("Couldn't get a connector %+v: %+v", c, err)
		} else {
			workersCount += len(config.tables)
			c.Run()
		}
	}

	writer.setWorkers(workersCount)
	writer.Run()
}