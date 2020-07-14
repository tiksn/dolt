// Copyright 2020 Liquidata, Inc.
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

package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
)

func TestHistoryTable(t *testing.T) {
	// history table test share a dEnv
	dEnv := setupHistoryTests(t)
	for _, test := range historyTableTests() {
		t.Run(test.name, func(t *testing.T) {
			testHistoryTable(t, test, dEnv)
		})
	}
}

var historyTableSetup = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (" +
		"pk int not null primary key," +
		"c0 int);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "first"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(0,0)," +
		"(1,1);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "second"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(2,2)," +
		"(3,3);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "third"}},
	{commands.SqlCmd{}, []string{"-q", "update test set c0 = c0+10 where c0 % 2 = 0"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "fourth"}},
	{commands.LogCmd{}, []string{}},
}

func historyTableTests() []integrationTest {
	return []integrationTest{
		{
			name:  "select pk, c0 from dolt_history_test",
			query: "select pk, c0 from dolt_history_test",
			rows: []sql.Row{
				{int32(0), int32(10)},
				{int32(1), int32(1)},
				{int32(2), int32(12)},
				{int32(3), int32(3)},
				{int32(0), int32(0)},
				{int32(1), int32(1)},
				{int32(2), int32(2)},
				{int32(3), int32(3)},
				{int32(0), int32(0)},
				{int32(1), int32(1)},
			},
		},
		{
			name:  "select commit_hash from dolt_history_test",
			query: "select commit_hash from dolt_history_test",
			rows: []sql.Row{
				{HEAD},
				{HEAD},
				{HEAD},
				{HEAD},
				{HEAD_1},
				{HEAD_1},
				{HEAD_1},
				{HEAD_1},
				{HEAD_2},
				{HEAD_2},
			},
		},
		{
			name:  "filter for a specific commit hash",
			query: "select pk, c0, commit_hash from dolt_history_test where commit_hash = hashof('head~1');",
			rows: []sql.Row{
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
			},
		},
		{
			name:  "filter out a specific commit hash",
			query: "select pk, c0, commit_hash from dolt_history_test where commit_hash != hashof('head~1');",
			rows: []sql.Row{
				{int32(0), int32(10), HEAD},
				{int32(1), int32(1), HEAD},
				{int32(2), int32(12), HEAD},
				{int32(3), int32(3), HEAD},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name: "compound or filter on commit hash",
			query: "select pk, c0, commit_hash from dolt_history_test " +
				"where commit_hash = hashof('head~1') or commit_hash = hashof('head~2');",
			rows: []sql.Row{
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name: "commit hash in value set",
			query: "select pk, c0, commit_hash from dolt_history_test " +
				"where commit_hash in (hashof('head~1'), hashof('head~2'));",
			rows: []sql.Row{
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name: "commit hash not in value set",
			query: "select pk, c0, commit_hash from dolt_history_test " +
				"where commit_hash not in (hashof('head~1'),hashof('head~2'));",
			rows: []sql.Row{
				{int32(0), int32(10), HEAD},
				{int32(1), int32(1), HEAD},
				{int32(2), int32(12), HEAD},
				{int32(3), int32(3), HEAD},
			},
		},
		{
			name:  "commit is not null",
			query: fmt.Sprintf("select pk, c0, commit_hash from dolt_history_test where commit_hash is not null;"),
			rows: []sql.Row{
				{int32(0), int32(10), HEAD},
				{int32(1), int32(1), HEAD},
				{int32(2), int32(12), HEAD},
				{int32(3), int32(3), HEAD},
				{int32(0), int32(0), HEAD_1},
				{int32(1), int32(1), HEAD_1},
				{int32(2), int32(2), HEAD_1},
				{int32(3), int32(3), HEAD_1},
				{int32(0), int32(0), HEAD_2},
				{int32(1), int32(1), HEAD_2},
			},
		},
		{
			name:  "commit is null",
			query: "select * from dolt_history_test where commit_hash is null;",
			rows:  []sql.Row{},
		},
	}
}

var HEAD = ""   // HEAD
var HEAD_1 = "" // HEAD~1
var HEAD_2 = "" // HEAD~2
var HEAD_3 = "" // HEAD~3
var INIT = ""   // HEAD~4

func setupHistoryTests(t *testing.T) *env.DoltEnv {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	for _, c := range historyTableSetup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	// get commit hashes from the log table
	q := "select commit_hash, date from dolt_log order by date desc;"
	rows, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, root, q)
	require.NoError(t, err)
	require.Equal(t, 5, len(rows))
	HEAD = rows[0][0].(string)
	HEAD_1 = rows[1][0].(string)
	HEAD_2 = rows[2][0].(string)
	HEAD_3 = rows[3][0].(string)
	INIT = rows[4][0].(string)

	return dEnv
}

func testHistoryTable(t *testing.T, test integrationTest, dEnv *env.DoltEnv) {
	ctx := context.Background()

	// history table tests share a dEnv and therefore must
	// be read-only. Put all setup in historyTableSetup.
	require.Empty(t, test.setup)

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	actRows, err := executeSelect(dEnv, dEnv.DoltDB, root, test.query)
	require.NoError(t, err)

	require.Equal(t, len(test.rows), len(actRows))
	for i := range test.rows {
		assert.Equal(t, test.rows[i], actRows[i])
	}
}
