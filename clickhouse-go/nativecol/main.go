/**
 * @Author: jiaweisi  2023/3/8 15:08
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
	batch                    *int
	debug                    *bool
)

var (
	ccollectorip, cdevip, cdevtype, cdstip, ceventmsg, ceventname, ceventtype, cflowdir, cobject, coperation, csrcip, csrclogtype,
	logtype_tag, manufacturer, reason []string
	iappprotocol, icollecttype, idstport, ieventlevel, imergecount, iprotocol, isrcport                                       []int32
	lduration, lrecivepack, lsendpack, request_body_len, response_body_len, lendtime, lid, loccurtime, lrecepttime, lstartime []int64
)

func main() {
	host = flag.String("host", "localhost:9000", "clickhouse host")
	user = flag.String("user", "test", "clichouse user")
	pass = flag.String("password", "test", "clickhouse password")
	db = flag.String("db", "test", "clickhouse database")
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
	for i := 0; i < *batch; i++ {
		ccollectorip = append(ccollectorip, data.InsertData.Ccollectorip)
		cdevip = append(cdevip, data.InsertData.Cdevip)
		cdevtype = append(cdevtype, data.InsertData.Cdevtype)
		cdstip = append(cdstip, data.InsertData.Cdstip)
		ceventmsg = append(ceventmsg, data.InsertData.Ceventmsg)
		ceventname = append(ceventname, data.InsertData.Ceventname)
		ceventtype = append(ceventtype, data.InsertData.Ceventtype)
		cflowdir = append(cflowdir, data.InsertData.Cflowdir)
		cobject = append(cobject, data.InsertData.Cobject)
		coperation = append(coperation, data.InsertData.Coperation)
		csrcip = append(csrcip, data.InsertData.Csrcip)
		csrclogtype = append(csrclogtype, data.InsertData.Csrclogtype)
		logtype_tag = append(logtype_tag, data.InsertData.LogypeTag)
		manufacturer = append(manufacturer, data.InsertData.Manufacturer)
		reason = append(reason, data.InsertData.Reason)
		iappprotocol = append(iappprotocol, data.InsertData.Iappprotocol)
		icollecttype = append(icollecttype, data.InsertData.Icollecttype)
		idstport = append(idstport, data.InsertData.Idstport)
		ieventlevel = append(ieventlevel, data.InsertData.Ieventlevel)
		imergecount = append(imergecount, data.InsertData.Imergecount)
		iprotocol = append(iprotocol, data.InsertData.Iprotocol)
		isrcport = append(isrcport, data.InsertData.Iprotocol)
		lduration = append(lduration, data.InsertData.Lduration)
		lrecivepack = append(lrecivepack, data.InsertData.Lrecivepack)
		lsendpack = append(lsendpack, data.InsertData.Lsendpack)
		request_body_len = append(request_body_len, data.InsertData.RequestBodyLen)
		response_body_len = append(response_body_len, data.InsertData.ResponseBodyLen)
		lendtime = append(lendtime, data.InsertData.Lendtime)
		lid = append(lid, data.InsertData.Lid)
		loccurtime = append(loccurtime, data.InsertData.Loccurtime)
		lrecepttime = append(lrecepttime, data.InsertData.Lrecepttime)
		lstartime = append(lstartime, data.InsertData.Lstartime)
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

func insert(conn driver.Conn, batch int) {
	start := time.Now().UnixNano()
	ctx := context.Background()
	bt, err := conn.PrepareBatch(ctx, data.InsertSql)
	if err != nil {
		log.Fatal(err)
	}
	s := time.Now().UnixNano()

	bt.Column(0).Append(ccollectorip)
	bt.Column(1).Append(cdevip)
	bt.Column(2).Append(cdevtype)
	bt.Column(3).Append(cdstip)
	bt.Column(4).Append(ceventmsg)
	bt.Column(5).Append(ceventname)
	bt.Column(6).Append(ceventtype)
	bt.Column(7).Append(cflowdir)
	bt.Column(8).Append(cobject)
	bt.Column(9).Append(coperation)
	bt.Column(10).Append(csrcip)
	bt.Column(11).Append(csrclogtype)
	bt.Column(12).Append(logtype_tag)
	bt.Column(13).Append(manufacturer)
	bt.Column(14).Append(reason)
	bt.Column(15).Append(iappprotocol)
	bt.Column(16).Append(icollecttype)
	bt.Column(17).Append(idstport)
	bt.Column(18).Append(ieventlevel)
	bt.Column(19).Append(imergecount)
	bt.Column(20).Append(iprotocol)
	bt.Column(21).Append(isrcport)
	bt.Column(22).Append(lduration)
	bt.Column(23).Append(lrecivepack)
	bt.Column(24).Append(lsendpack)
	bt.Column(25).Append(request_body_len)
	bt.Column(26).Append(response_body_len)
	bt.Column(27).Append(lendtime)
	bt.Column(28).Append(lid)
	bt.Column(29).Append(loccurtime)
	bt.Column(30).Append(lrecepttime)
	bt.Column(31).Append(lstartime)
	e := time.Now().UnixNano()
	bt.Send()
	end := time.Now().UnixNano()
	fmt.Printf("记录数: %d, 耗时: %d, 循环准备耗时: %d, 速率: %d(条/秒) \n", batch, (end-start)/1e6, (e-s)/1e6, int64(batch*1000000*1000)/(end-start))
}
