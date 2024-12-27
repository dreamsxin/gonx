//go:build exclude
// +build exclude

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	gonx "github.com/dreamsxin/gonx"
)

var format string
var logFile string

func init() {
	flag.StringVar(&format, "format", `$remote_addr [$time_local] "$request" $status $request_length $body_bytes_sent $request_time "$t_size" $read_time $gen_time`, "Log format")
	flag.StringVar(&logFile, "log", "dummy", "Log file name to read. Read from STDIN if file name is '-'")
}

func main() {
	flag.Parse()

	// Create a parser based on given format
	parser := gonx.NewParser(format)

	// Make a chain of reducers to get some stats from log file
	{
		// Read given file or from STDIN
		var logReader io.Reader
		if logFile == "dummy" {
			logReader = strings.NewReader(`89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
	89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
	89.234.89.124 [08/Nov/2013:13:39:19 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 400 1027 2430 0.015 "100x100" 11 2
	89.234.89.125 [08/Nov/2013:13:39:20 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 500 1027 2430 0.015 "100x100" 11 2`)
		} else if logFile == "-" {
			logReader = os.Stdin
		} else {
			file, err := os.Open(logFile)
			if err != nil {
				panic(err)
			}
			logReader = file
			defer file.Close()
		}

		reducer := gonx.NewGroupBy(
			[]string{"remote_addr"},
			&gonx.Avg{map[string]string{"request_time": "request_time", "read_time": "read_time", "gen_time": "gen_time"}},
			&gonx.Sum{map[string]string{"body_bytes_sent": "body_bytes_sent"}},
			&gonx.Count{},
		)
		output := gonx.MapReduce(logReader, parser, reducer)
		for res := range output {
			// Process the record... e.g.
			fmt.Printf("Parsed entry: %+v\n", res)
		}
	}

	{

		// Read given file or from STDIN
		var logReader io.Reader
		if logFile == "dummy" {
			logReader = strings.NewReader(`89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
	89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
	89.234.89.124 [08/Nov/2013:13:39:19 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 400 1027 2430 0.015 "100x100" 11 2
	89.234.89.125 [08/Nov/2013:13:39:20 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 500 1027 2430 0.015 "100x100" 11 2`)
		} else if logFile == "-" {
			logReader = os.Stdin
		} else {
			file, err := os.Open(logFile)
			if err != nil {
				panic(err)
			}
			logReader = file
			defer file.Close()
		}

		reducer := gonx.NewChain(
			&gonx.Avg{map[string]string{"avg_request_time": "request_time", "read_time": "read_time", "gen_time": "gen_time"}},
			&gonx.Sum{map[string]string{"body_bytes_sent": "body_bytes_sent"}},
			&gonx.Min{map[string]string{"min_request_time": "request_time"}},
			&gonx.Max{map[string]string{"max_request_time": "request_time"}},
			&gonx.Count{},
		)
		output := gonx.MapReduce(logReader, parser, reducer)
		for res := range output {
			// Process the record... e.g.
			fmt.Printf("Parsed entry: %+v\n", res)
		}
	}

	{
		// Read given file or from STDIN
		var logReader io.Reader
		if logFile == "dummy" {
			logReader = strings.NewReader(`89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
	89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
	89.234.89.124 [08/Nov/2013:13:39:19 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 400 1027 2430 0.015 "100x100" 11 2
	89.234.89.125 [08/Nov/2013:13:39:20 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 500 1027 2430 0.015 "100x100" 11 2`)
		} else if logFile == "-" {
			logReader = os.Stdin
		} else {
			file, err := os.Open(logFile)
			if err != nil {
				panic(err)
			}
			logReader = file
			defer file.Close()
		}

		reducer := gonx.NewGroupBy(
			[]string{"remote_addr"},
			&gonx.Avg{map[string]string{"request_time": "request_time", "read_time": "read_time", "gen_time": "gen_time"}},
			&gonx.Sum{map[string]string{"body_bytes_sent": "body_bytes_sent"}},
			&gonx.Count{},
		)
		output := gonx.MapReduce(logReader, parser, reducer)

		reducer2 := gonx.NewChain(
			&gonx.Count{},
		)

		var output2 = make(chan *gonx.Entry)
		go reducer2.Reduce(output, output2)
		for res := range output2 {
			fmt.Printf("Parsed entry: %+v\n", res)
		}
	}

	{
		// Read given file or from STDIN
		var logReader io.Reader
		if logFile == "dummy" {
			logReader = strings.NewReader(`89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.014 "100x100" 10 1
89.234.89.124 [08/Nov/2013:13:39:19 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 400 1027 2430 0.015 "100x100" 11 2
89.234.89.125 [08/Nov/2013:13:39:20 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 500 1027 2430 0.015 "100x100" 11 2`)
		} else if logFile == "-" {
			logReader = os.Stdin
		} else {
			file, err := os.Open(logFile)
			if err != nil {
				panic(err)
			}
			logReader = file
			defer file.Close()
		}

		filterreducer := gonx.NewTogether(
			"status",
			gonx.NewGroupBy(
				[]string{"status"},
				&gonx.Avg{map[string]string{"avg_rtime": "request_time"}},
				&gonx.Count{},
				&gonx.ReducerHistogram{Fields: map[string]string{"histogram": "request_time"}, Bins: map[string]*gonx.Bin{"histogram": gonx.NewBin(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)}},
			),
		)

		reducer := gonx.NewChain(
			&gonx.Avg{Fields: map[string]string{"total_avg_rtime": "request_time"}},
			&gonx.Count{},
			&gonx.ReducerHistogram{Fields: map[string]string{"total_histogram": "request_time"}, Bins: map[string]*gonx.Bin{"total_histogram": gonx.NewBin(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)}},
			filterreducer,
		)
		output := gonx.MapReduce(logReader, parser, reducer)
		for res := range output {
			b, _ := json.Marshal(res)
			fmt.Printf("Parsed entry: %+v\n", string(b))
		}
	}
}
