/**
 * @Author: jiaweisi  2023/3/8 11:27
 * @File:  main
 * @Software: GoLand
 * @Updater:         2023/3/8
 * @Description:
 */

package main

import (
	"clickhouse/data"
	"flag"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	host, user, pass, db, tb *string
	batch                    *int
)

func main() {
	host = flag.String("host", "10.5.30.249:9000", "clickhouse host")
	user = flag.String("user", "pangu", "clichouse user")
	pass = flag.String("password", "pangu.CK@2021", "clickhouse password")
	db = flag.String("db", "pangu", "clickhouse database")
	batch = flag.Int("batch", 100000, "clickhouse batch")
	tb = flag.String("tb", "event", "clickhouse table")
	flag.Parse()
	fmt.Printf("clickhouse host: %s \nclichouse user: %s \nclickhouse password: %s \nclickhouse database: %s \nclickhouse batch: %d \nclickhouse table: %s \n",
		*host, *user, *pass, *db, *batch, *tb)
	conn, err := sqlx.Connect("clickhouse", fmt.Sprintf("tcp://%s?username=%s&password=%s&database=%s&debug=false", *host, *user, *pass, *db))
	if err != nil {
		panic(any(err))
	}
	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(10)
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

func insert(conn *sqlx.DB, batch int) {
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

