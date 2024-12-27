//go:build exclude
// +build exclude

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	gonx "github.com/dreamsxin/gonx"
)

var format string
var logFile string

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&format, "format", `$remote_addr [$time_local] "$request" $status $request_length $body_bytes_sent $request_time "$t_size" $read_time $gen_time`, "Log format")
	flag.StringVar(&logFile, "log", "dummy", "Log file name to read. Read from STDIN if file name is '-'")
}

func main() {
	flag.Parse()

	// Create a parser based on given format
	parser := gonx.NewParser(format)

	{
		// Read given file or from STDIN
		var logReader io.Reader
		if logFile == "dummy" {
			logReader = strings.NewReader(`89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.1 "100x100" 10 1
89.234.89.123 [08/Nov/2013:13:39:18 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 200 1027 2430 0.2 "100x100" 10 1
89.234.89.124 [08/Nov/2013:13:39:19 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 400 1027 2430 0.40 "100x100" 11 2
89.234.89.125 [08/Nov/2013:13:39:20 +0000] "GET /t/100x100/foo/bar.jpeg HTTP/1.1" 500 1027 2430 0.50 "100x100" 11 2`)
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
				&gonx.Avg{map[string]string{"avg_request_time": "request_time"}},
				&gonx.Count{},
			),
		)

		reducer := gonx.NewChain(
			&gonx.Avg{Fields: map[string]string{"total_avg_request_time": "request_time"}},
			&gonx.Count{},
			filterreducer,
		)

		output := gonx.MapReduce(logReader, parser, reducer)
		for res := range output {
			// Process the record... e.g.
			fmt.Printf("Parsed entry: %+v\n", res)
		}
	}

}
