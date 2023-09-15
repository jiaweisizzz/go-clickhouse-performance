/**
 * @Author: jiaweisi  2023/3/8 14:48
 * @File:  main
 * @Software: GoLand
 * @Updater:         2023/3/8
 * @Description:
 */

package main

import (
	"clickhouse/data"
	"context"
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	host, user, pass, db, tb *string
	batch, g                 *int
	debug                    *bool
)

func main() {
	host = flag.String("host", "localhost:9000", "clickhouse host")
	user = flag.String("user", "test", "clichouse user")
	pass = flag.String("password", "test", "clickhouse password")
	db = flag.String("db", "test", "clickhouse database")
	g = flag.Int("g", 1, "write goroutine")
	batch = flag.Int("batch", 100000, "clickhouse batch")
	tb = flag.String("tb", "event", "clickhouse table")
	debug = flag.Bool("debug", false, "clickhouse debug")
	flag.Parse()
	fmt.Printf("clickhouse host: %s \nclichouse user: %s \nclickhouse password: %s \nclickhouse database: %s \nclickhouse batch: %d \nclickhouse table: %s \n",
		*host, *user, *pass, *db, *batch, *tb)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*host},
		Auth: clickhouse.Auth{
			Database: *db,
			Username: *user,
			Password: *pass,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		MaxOpenConns: 50,
		MaxIdleConns: 10,
		DialTimeout:  5 * time.Second,
		Debug:        *debug,
	})
	if err != nil {
		panic(any(err))
	}
	if err := conn.Ping(context.Background()); err != nil {
		panic(any(err))
	}
	for i := 0; i < *g; i++ {
		go func() {
			for {
				insert(conn, *batch)
			}
		}()
	}
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	}
}

func insert(conn driver.Conn, batch int) {
	start := time.Now().UnixNano()
	ctx := context.Background()
	bt, err := conn.PrepareBatch(ctx, data.InsertSql)
	if err != nil {
		log.Fatal(err)
	}
	s := time.Now().UnixNano()
	for i := 0; i < batch; i++ {
		err := bt.Append(data.InsertData.Ccollectorip, data.InsertData.Cdevip, data.InsertData.Cdevtype, data.InsertData.Cdstip, data.InsertData.Ceventmsg,
			data.InsertData.Ceventname, data.InsertData.Ceventtype, data.InsertData.Cflowdir, data.InsertData.Cobject, data.InsertData.Coperation,
			data.InsertData.Csrcip, data.InsertData.Csrclogtype, data.InsertData.LogypeTag, data.InsertData.Manufacturer, data.InsertData.Reason,
			data.InsertData.Iappprotocol, data.InsertData.Icollecttype, data.InsertData.Idstport, data.InsertData.Ieventlevel, data.InsertData.Imergecount,
			data.InsertData.Iprotocol, data.InsertData.Isrcport, data.InsertData.Lduration, data.InsertData.Lrecivepack, data.InsertData.Lsendpack,
			data.InsertData.RequestBodyLen, data.InsertData.ResponseBodyLen, data.InsertData.Lendtime, data.InsertData.Lid, time.Now().UnixNano()/1e6,
			time.Now().UnixNano()/1e6, data.InsertData.Lstartime, time.Now().Format("2006-01-02 15:04:05"))
		if err != nil {
			log.Fatal(err)
		}
	}
	e := time.Now().UnixNano()
	bt.Send()
	end := time.Now().UnixNano()
	fmt.Printf("记录数: %d, 耗时: %d, 循环准备耗时: %d, 速率: %d(条/秒) \n", batch, (end-start)/1e6, (e-s)/1e6, int64(batch*1000000*1000)/(end-start))
}
