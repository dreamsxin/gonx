//go:build exclude
// +build exclude

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/dreamsxin/gonx"
	"github.com/dreamsxin/gonx/follower"
)

var format string
var logFile string

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&format, "format", `$remote_addr [$time_local] "$request" $status $request_length $body_bytes_sent $request_time "$t_size" $read_time $gen_time`, "Log format")
	flag.StringVar(&logFile, "log", "access.log", "Log file name")
}

var (
	locker sync.Mutex
	logs   []string
)

func main() {

	// Create a parser based on given format
	parser := gonx.NewParser(format)

	t, err := follower.New(logFile, follower.Config{
		Whence: io.SeekStart,
		Offset: 0,
		Reopen: true,
	})
	if err != nil {
		panic(err)
	}

	reducer := gonx.NewChain(
		&gonx.Avg{Fields: map[string]string{"request_time": "request_time", "read_time": "read_time", "gen_time": "gen_time"}},
		&gonx.Sum{Fields: map[string]string{"body_bytes_sent": "body_bytes_sent"}},
		&gonx.Count{},
	)
	var wg sync.WaitGroup
	var entries chan *gonx.Entry
	var lasttime time.Time
	for line := range t.Lines() {
		//log.Println(line.String())
		entry, err := parser.ParseString(line.String())
		if err == nil {
			time_local, err := entry.StringField("time_local")
			if err != nil {
				log.Println(err)
				continue
			}
			t, err := time.ParseInLocation("02/Jan/2006:15:04:05 -0700", time_local, time.Local)
			if err != nil {
				log.Println(err)
				continue
			}
			if lasttime.Format("2006-01-02") != t.Format("2006-01-02") {
				if entries != nil {
					close(entries)
				}

				log.Println("start new day", t.Format("2006-01-02"))
				entries = make(chan *gonx.Entry, 10)
				wg.Add(1)
				go func(t time.Time, entries chan *gonx.Entry) {
					defer wg.Done()
					var output = make(chan *gonx.Entry)
					go reducer.Reduce(entries, output)
					for res := range output {
						// Process the record... e.g.
						fmt.Printf("Parsed entry: %s, %+v\n", t.Format("2006-01-02"), res)
					}

				}(t, entries)
			}
			lasttime = t
			entries <- entry
		} else {
			log.Println(err)
		}
	}
	wg.Wait()
}
