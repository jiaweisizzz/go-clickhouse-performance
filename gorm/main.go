/**
 * @Author: jiaweisi  2023/3/8 16:31
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
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
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
	host = flag.String("host", "localhost:9000", "clickhouse host")
	user = flag.String("user", "test", "clichouse user")
	pass = flag.String("password", "test", "clickhouse password")
	db = flag.String("db", "test", "clickhouse database")
	batch = flag.Int("batch", 100000, "clickhouse batch")
	tb = flag.String("tb", "event", "clickhouse table")
	flag.Parse()
	fmt.Printf("clickhouse host: %s \nclichouse user: %s \nclickhouse password: %s \nclickhouse database: %s \nclickhouse batch: %d \nclickhouse table: %s \n",
		*host, *user, *pass, *db, *batch, *tb)
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s/%s", *user, *pass, *host, *db)
	db, err := gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(any(err))
	}
	cdb, err := db.DB()
	if err != nil {
		panic(any(err))
	}
	cdb.SetMaxOpenConns(50)
	cdb.SetMaxIdleConns(10)
	go func() {
		for {
			insert(db, *batch)
		}
	}()
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
	}
}

func insert(db *gorm.DB, batch int) {
	start := time.Now().UnixNano()
	result := make([]data.Data, 0, batch)
	for i := 0; i < 100000; i++ {
		result = append(result, data.InsertData)
	}
	db.Table("event").Create(&result)
	end := time.Now().UnixNano()
	fmt.Printf("记录数: %d, 耗时: %d, 速率: %d(条/秒) \n", 100000, (end-start)/1e6, int64(100000*1000000*1000)/(end-start))

}
