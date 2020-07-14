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
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dtestutils"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
)

func TestDiffTable(t *testing.T) {
	for _, test := range diffTableTests {
		t.Run(test.name, func(t *testing.T) {
			testDiffTable(t, test)
		})
	}
}

var setupDiffTests = []testCommand{
	{commands.SqlCmd{}, []string{"-q", "create table test (" +
		"pk int not null primary key," +
		"c0 int);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "created table"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(0,0)," +
		"(1,1);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "added two rows"}},
	{commands.BranchCmd{}, []string{"other"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(2,2)," +
		"(3,3);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "added two more rows on master"}},
	{commands.CheckoutCmd{}, []string{"other"}},
	{commands.SqlCmd{}, []string{"-q", "insert into test values " +
		"(8,8)," +
		"(9,9);"}},
	{commands.AddCmd{}, []string{"."}},
	{commands.CommitCmd{}, []string{"-m", "added two more rows on other"}},
	{commands.CheckoutCmd{}, []string{"master"}},
}

var diffTableTests = []integrationTest{
	{
		name:  "select to_pk, to_c0, from_pk, from_c0 from dolt_diff_test",
		query: "select to_pk, to_c0, from_pk, from_c0 from dolt_diff_test;",
		rows: []sql.Row{
			{int32(2), int32(2), nil, nil},
			{int32(3), int32(3), nil, nil},
			{int32(0), int32(0), nil, nil},
			{int32(1), int32(1), nil, nil},
		},
	},
}

func testDiffTable(t *testing.T, test integrationTest) {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()

	for _, c := range setupDiffTests {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}
	for _, c := range test.setup {
		exitCode := c.cmd.Exec(ctx, c.cmd.Name(), c.args, dEnv)
		require.Equal(t, 0, exitCode)
	}

	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)

	actRows, err := sqle.ExecuteSelect(dEnv, dEnv.DoltDB, root, test.query)
	require.NoError(t, err)

	require.Equal(t, len(test.rows), len(actRows))
	for i := range test.rows {
		assert.Equal(t, test.rows[i], actRows[i])
	}
}
