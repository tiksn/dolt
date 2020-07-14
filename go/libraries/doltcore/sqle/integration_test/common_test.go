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
	"io"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	. "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/dfunctions"
	sqle "github.com/liquidata-inc/go-mysql-server"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/analyzer"
)

// borrowed from sqle/testutil, but with a different test engine provider.
func executeSelect(dEnv *env.DoltEnv, ddb *doltdb.DoltDB, root *doltdb.RootValue, query string) ([]sql.Row, error) {
	db := NewDatabase("dolt", ddb, dEnv.RepoState, dEnv.RepoStateWriter())
	engine, ctx, err := newTestEngineWithCatalog(context.Background(), db, root)
	if err != nil {
		return nil, err
	}

	_, rowIter, err := engine.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var (
		rows   []sql.Row
		rowErr error
		row    sql.Row
	)
	for row, rowErr = rowIter.Next(); rowErr == nil; row, rowErr = rowIter.Next() {
		rows = append(rows, row)
	}

	if rowErr != io.EOF {
		return nil, rowErr
	}

	return rows, nil
}

// newTestEngineWithCatalog creates a test sql engine that includes Dolt specific functions and the info schema db.
func newTestEngineWithCatalog(ctx context.Context, db Database, root *doltdb.RootValue) (*sqle.Engine, *sql.Context, error) {
	c := sql.NewCatalog()
	err := c.Register(dfunctions.DoltFunctions...)
	if err != nil {
		return nil, nil, err
	}

	engine := sqle.New(c, analyzer.NewDefault(c), &sqle.Config{})
	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
	engine.AddDatabase(db)

	sqlCtx := NewTestSQLCtx(ctx)
	DSessFromSess(sqlCtx.Session).AddDB(ctx, db)
	sqlCtx.SetCurrentDatabase(db.Name())
	err = db.SetRoot(sqlCtx, root)

	if err != nil {
		return nil, nil, err
	}

	err = RegisterSchemaFragments(sqlCtx, db, root)

	if err != nil {
		return nil, nil, err
	}

	return engine, sqlCtx, nil
}