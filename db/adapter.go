package db

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
)

type Adapter struct {
	db *sql.DB
}

func (a *Adapter) Exec(query string, args ...interface{}) (sql.Result, error) {
	return a.db.Exec(query, args...)
}

func (a *Adapter) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

func (a *Adapter) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, nil
}

func (a *Adapter) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return a.db.Query(query, args...)
}

func (a *Adapter) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

func (a *Adapter) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return a.db.QueryRowContext(ctx, query, args...)
}

func (a *Adapter) Tx(ctx context.Context) (*sql.Tx, error) {
	if ctx != nil {
		return a.db.BeginTx(ctx, nil)
	}
	return a.db.Begin()
}
