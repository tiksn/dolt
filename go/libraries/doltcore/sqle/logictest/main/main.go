// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"os"

	"github.com/liquidata-inc/sqllogictest/go/logictest"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/logictest/dolt"
	"flag"
	"log"

	"runtime"
	"runtime/pprof"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

// Runs all sqllogictest test files (or directories containing them) given as arguments.
// Usage: $command (run|parse) [version] [file1.test dir1/ dir2/]
// In run mode, runs the tests and prints results to stdout.
// In parse mode, parses test results from the file given and prints them to STDOUT in a format to be imported by dolt.
func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	h := &dolt.DoltHarness{}
	logictest.RunTestFiles(h, "SQLLOGICTESTS_PATH/test")

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func parseTestResults(version, f string) {
	entries, err := logictest.ParseResultFile(f)
	if err != nil {
		panic(err)
	}

	records := make([]*DoltResultRecord, len(entries))
	for i, e := range entries {
		records[i] = NewDoltRecordResult(e, version)
	}

	b, err := JSONMarshal(records)
	if err != nil {
		panic(err)
	}

	_, err = os.Stdout.Write(b)
	if err != nil {
		panic(err)
	}
}

// Custom json marshalling function is necessary to avoid escaping <, > and & to html unicode escapes
func JSONMarshal(records []*DoltResultRecord) ([]byte, error) {
	rows := &TestResultArray{Rows: records}
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(rows)
	return buffer.Bytes(), err
}

func NewDoltRecordResult(e *logictest.ResultLogEntry, version string) *DoltResultRecord {
	var result string
	switch e.Result {
	case logictest.Ok:
		result = "ok"
	case logictest.NotOk:
		result = "not ok"
	case logictest.Skipped:
		result = "skipped"
	case logictest.Timeout:
		result = "timeout"
	case logictest.DidNotRun:
		result = "did not run"
	}
	return &DoltResultRecord{
		Version:      version,
		TestFile:     e.TestFile,
		LineNum:      e.LineNum,
		Query:        e.Query,
		Duration:     e.Duration.Milliseconds(),
		Result:       result,
		ErrorMessage: e.ErrorMessage,
	}
}

type TestResultArray struct {
	Rows []*DoltResultRecord `json:"rows"`
}

type DoltResultRecord struct {
	Version      string `json:"version"`
	TestFile     string `json:"test_file"`
	LineNum      int    `json:"line_num"`
	Query        string `json:"query_string"`
	Duration     int64  `json:"duration"`
	Result       string `json:"result"`
	ErrorMessage string `json:"error_message,omitempty"`
}
