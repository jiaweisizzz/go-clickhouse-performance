/**
 * @Author: jiaweisi  2023/3/8 14:03
 * @File:  main
 * @Software: GoLand
 * @Updater:         2023/3/8
 * @Description:
 */

package main

import (
	"clickhouse/data"
	"database/sql"
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	host, user, pass, db, tb, debug *string
	batch                           *int
)

func main() {
	host = flag.String("host", "localhost:9000", "clickhouse host")
	user = flag.String("user", "test", "clichouse user")
	pass = flag.String("password", "test", "clickhouse password")
	db = flag.String("db", "test", "clickhouse database")
	batch = flag.Int("batch", 100000, "clickhouse batch")
	tb = flag.String("tb", "event", "clickhouse table")
	debug = flag.String("debug", "false", "clickhouse debug")
	flag.Parse()
	fmt.Printf("clickhouse host: %s \nclichouse user: %s \nclickhouse password: %s \nclickhouse database: %s \nclickhouse batch: %d \nclickhouse table: %s \n",
		*host, *user, *pass, *db, *batch, *tb)
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{*host},
		Auth: clickhouse.Auth{
			Database: *db,
			Username: *user,
			Password: *pass,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Debug:       true,
	})
	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(10)
	if err := conn.Ping(); err != nil {
		panic(any(err))
	}
	go func() {
		for {
			insert(conn, *batch)
		}
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	}
}

func insert(conn *sql.DB, batch int) {
	start := time.Now().UnixNano()
	tx, err := conn.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare(data.InsertSql)
	if err != nil {
		log.Fatal(err)
	}
	s := time.Now().UnixNano()
	for i := 0; i < batch; i++ {
		_, err := stmt.Exec(data.InsertData.Ccollectorip, data.InsertData.Cdevip, data.InsertData.Cdevtype, data.InsertData.Cdstip, data.InsertData.Ceventmsg,
			data.InsertData.Ceventname, data.InsertData.Ceventtype, data.InsertData.Cflowdir, data.InsertData.Cobject, data.InsertData.Coperation,
			data.InsertData.Csrcip, data.InsertData.Csrclogtype, data.InsertData.LogypeTag, data.InsertData.Manufacturer, data.InsertData.Reason,
			data.InsertData.Iappprotocol, data.InsertData.Icollecttype, data.InsertData.Idstport, data.InsertData.Ieventlevel, data.InsertData.Imergecount,
			data.InsertData.Iprotocol, data.InsertData.Isrcport, data.InsertData.Lduration, data.InsertData.Lrecivepack, data.InsertData.Lsendpack,
			data.InsertData.RequestBodyLen, data.InsertData.ResponseBodyLen, data.InsertData.Lendtime, data.InsertData.Lid, data.InsertData.Loccurtime,
			data.InsertData.Lrecepttime, data.InsertData.Lstartime)
		if err != nil {
			log.Fatal(err)
		}
	}
	e := time.Now().UnixNano()
	tx.Commit()
	stmt.Close()
	end := time.Now().UnixNano()
	fmt.Printf("记录数: %d, 耗时: %d, 循环准备耗时: %d, 速率: %d(条/秒) \n", batch, (end-start)/1e6, (e-s)/1e6, int64(batch*1000000*1000)/(end-start))
}
