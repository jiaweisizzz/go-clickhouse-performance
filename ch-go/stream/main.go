/**
 * @Author: jiaweisi  2023/3/8 17:16
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
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	host, user, pass, db, tb *string
	batch, block             *int
)

var insertSql = `INSERT INTO event (ccollectorip, cdevip, cdevtype, cdstip, ceventmsg, ceventname, ceventtype, cflowdir,
cobject, coperation, csrcip, csrclogtype, logtype_tag, manufacturer, reason, iappprotocol, icollecttype, idstport,
ieventlevel, imergecount, iprotocol, isrcport, lduration, lrecivepack, lsendpack, request_body_len, response_body_len,
lendtime, lid, loccurtime, lrecepttime, lstartime) VALUES`

func main() {
	host = flag.String("host", "localhost:9000", "clickhouse host")
	user = flag.String("user", "test", "clichouse user")
	pass = flag.String("password", "test", "clickhouse password")
	db = flag.String("db", "test", "clickhouse database")
	batch = flag.Int("batch", 100000, "clickhouse batch")
	tb = flag.String("tb", "event", "clickhouse table")
	block = flag.Int("block", 1, "stream block")
	flag.Parse()
	fmt.Printf("clickhouse host: %s \nclichouse user: %s \nclickhouse password: %s \nclickhouse database: %s \nclickhouse batch: %d \nclickhouse table: %s \n",
		*host, *user, *pass, *db, *batch, *tb)
	conn, err := ch.Dial(context.Background(),
		ch.Options{
			Address:  *host,
			Database: *db,
			User:     *user,
			Password: *pass,
		})
	if err != nil {
		panic(any(err))
	}
	if err := conn.Ping(context.Background()); err != nil {
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

func insert(conn *ch.Client, batch int) {
	start := time.Now().UnixNano()
	var blocks int
	var (
		ccollectorip, cdevip, cdevtype, cdstip, ceventmsg, ceventname, ceventtype, cflowdir, cobject, coperation, csrcip, csrclogtype,
		logtype_tag, manufacturer, reason proto.ColStr
		iappprotocol, icollecttype, idstport, ieventlevel, imergecount, iprotocol, isrcport                                       proto.ColInt32
		lduration, lrecivepack, lsendpack, request_body_len, response_body_len, lendtime, lid, loccurtime, lrecepttime, lstartime proto.ColInt64
	)
	input := proto.Input{
		{Name: "ccollectorip", Data: &ccollectorip},
		{Name: "cdevip", Data: &cdevip},
		{Name: "cdevtype", Data: &cdevtype},
		{Name: "cdstip", Data: &cdstip},
		{Name: "ceventmsg", Data: &ceventmsg},
		{Name: "ceventname", Data: &ceventname},
		{Name: "ceventtype", Data: &ceventtype},
		{Name: "cflowdir", Data: &cflowdir},
		{Name: "cobject", Data: &cobject},
		{Name: "coperation", Data: &coperation},
		{Name: "csrcip", Data: &csrcip},
		{Name: "csrclogtype", Data: &csrclogtype},
		{Name: "logtype_tag", Data: &logtype_tag},
		{Name: "manufacturer", Data: &manufacturer},
		{Name: "reason", Data: &reason},
		{Name: "iappprotocol", Data: &iappprotocol},
		{Name: "icollecttype", Data: &icollecttype},
		{Name: "idstport", Data: &idstport},
		{Name: "ieventlevel", Data: &ieventlevel},
		{Name: "imergecount", Data: &imergecount},
		{Name: "iprotocol", Data: &iprotocol},
		{Name: "isrcport", Data: &isrcport},
		{Name: "lduration", Data: &lduration},
		{Name: "lrecivepack", Data: &lrecivepack},
		{Name: "lsendpack", Data: &lsendpack},
		{Name: "request_body_len", Data: &request_body_len},
		{Name: "response_body_len", Data: &response_body_len},
		{Name: "lendtime", Data: &lendtime},
		{Name: "lid", Data: &lid},
		{Name: "loccurtime", Data: &loccurtime},
		{Name: "lrecepttime", Data: &lrecepttime},
		{Name: "lstartime", Data: &lstartime},
	}
	if err := conn.Do(context.Background(), ch.Query{
		Body:  insertSql,
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()

			if blocks >= *block {
				return io.EOF
			}
			for i := 0; i < batch; i++ {
				ccollectorip.Append(data.InsertData.Ccollectorip)
				cdevip.Append(data.InsertData.Cdevip)
				cdevtype.Append(data.InsertData.Cdevtype)
				cdstip.Append(data.InsertData.Cdstip)
				ceventmsg.Append(data.InsertData.Ceventmsg)
				ceventname.Append(data.InsertData.Ceventname)
				ceventtype.Append(data.InsertData.Ceventtype)
				cflowdir.Append(data.InsertData.Cflowdir)
				cobject.Append(data.InsertData.Cobject)
				coperation.Append(data.InsertData.Coperation)
				csrcip.Append(data.InsertData.Csrcip)
				csrclogtype.Append(data.InsertData.Csrclogtype)
				logtype_tag.Append(data.InsertData.LogypeTag)
				manufacturer.Append(data.InsertData.Manufacturer)
				reason.Append(data.InsertData.Reason)
				iappprotocol.Append(data.InsertData.Iappprotocol)
				icollecttype.Append(data.InsertData.Icollecttype)
				idstport.Append(data.InsertData.Idstport)
				ieventlevel.Append(data.InsertData.Ieventlevel)
				imergecount.Append(data.InsertData.Imergecount)
				iprotocol.Append(data.InsertData.Iprotocol)
				isrcport.Append(data.InsertData.Isrcport)
				lduration.Append(data.InsertData.Lduration)
				lrecivepack.Append(data.InsertData.Lrecivepack)
				lsendpack.Append(data.InsertData.Lsendpack)
				request_body_len.Append(data.InsertData.RequestBodyLen)
				response_body_len.Append(data.InsertData.ResponseBodyLen)
				lendtime.Append(data.InsertData.Lendtime)
				lid.Append(data.InsertData.Lid)
				loccurtime.Append(data.InsertData.Loccurtime)
				lrecepttime.Append(data.InsertData.Lrecepttime)
				lstartime.Append(data.InsertData.Lstartime)
			}

			blocks++
			return nil
		},
	}); err != nil {
		log.Fatal(err)
	}
	end := time.Now().UnixNano()
	fmt.Printf("记录数: %d, 耗时: %d, 速率: %d(条/秒) \n", batch*(*block), (end-start)/1e6, int64(batch*(*block)*1000000*1000)/(end-start))
}
