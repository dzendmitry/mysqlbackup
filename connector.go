package main

import (
	"github.com/dzendmitry/logger"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

type connector struct {
	config config
	db *sql.DB
	writerCmdCh chan writerCmd
	log logger.ILogger
}

func NewConnector(config config, writerCmdCh chan writerCmd) (*connector, error) {
	c := connector{
		config: config,
		writerCmdCh: writerCmdCh,
		log: logger.InitConsoleLogger(fmt.Sprint("CONNECTOR-%v", config.name)),
	}
	var err error
	if c.db, err = sql.Open("mysql", config.server); err != nil {
		return nil, err
	}
	c.db.SetMaxIdleConns(config.connPoolSize)
	c.db.SetMaxOpenConns(config.connPoolSize)
	return &c, nil
}

func (c *connector) Run() {
	for _, table := range c.config.tables {
		GetWorker(c, table).Run()
	}
}