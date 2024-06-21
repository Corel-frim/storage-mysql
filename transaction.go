package mysql

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"unsafe"

	"github.com/go-qbit/qerror"
	"github.com/go-qbit/timelog"
)

const ctx_transaction_key = "MYSQL_TRANSACTION"

type transaction struct {
	tx           *sql.Tx
	savePoint    uint64
	savePointMtx sync.Mutex
}

func (s *MySQL) StartTransaction(ctx context.Context) (context.Context, error) {
	t := ctx.Value(s.transactionKey())

	if t == nil {
		if debugSQL {
			println("BEGIN")
		}
		ctx = timelog.Start(ctx, "BEGIN")
		tx, err := s.db.Begin()
		ctx = timelog.Finish(ctx)
		if err != nil {
			return nil, err
		}

		return context.WithValue(ctx, s.transactionKey(), &transaction{
			tx: tx,
		}), nil
	} else {
		t := t.(*transaction)
		t.savePointMtx.Lock()
		defer t.savePointMtx.Unlock()

		t.savePoint++

		if debugSQL {
			println("SAVEPOINT SP" + strconv.FormatUint(t.savePoint, 10))
		}
		_, err := t.tx.Exec("SAVEPOINT SP" + strconv.FormatUint(t.savePoint, 10))
		if err != nil {
			return nil, err
		}

		return ctx, nil
	}
}

func (s *MySQL) UseTransaction(ctx context.Context, tx *sql.Tx) (context.Context, error) {
	if tx == nil {
		return nil, qerror.Errorf("No transaction provided")
	}

	t := ctx.Value(s.transactionKey())
	if t != nil {
		return nil, qerror.Errorf("Transaction already started")
	}

	return context.WithValue(ctx, s.transactionKey(), &transaction{
		tx: tx,
	}), nil
}

func (s *MySQL) Commit(ctx context.Context) (context.Context, error) {
	ct := ctx.Value(s.transactionKey())

	if ct == nil {
		return nil, qerror.Errorf("No started transaction")
	}

	t := ct.(*transaction)
	t.savePointMtx.Lock()
	defer t.savePointMtx.Unlock()

	if t.savePoint > 0 {
		if debugSQL {
			println("RELEASE SAVEPOINT SP" + strconv.FormatUint(t.savePoint, 10))
		}
		_, err := t.tx.Exec("RELEASE SAVEPOINT SP" + strconv.FormatUint(t.savePoint, 10))
		if err != nil {
			return nil, err
		}

		t.savePoint--

		return ctx, nil
	}

	if debugSQL {
		println("COMMIT")
	}
	ctx = timelog.Start(ctx, "COMMIT")
	err := t.tx.Commit()
	ctx = timelog.Finish(ctx)
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, s.transactionKey(), nil), nil
}

func (s *MySQL) Rollback(ctx context.Context) (context.Context, error) {
	ct := ctx.Value(s.transactionKey())

	if ct == nil {
		return nil, qerror.Errorf("No started transaction")
	}

	t := ct.(*transaction)
	t.savePointMtx.Lock()
	defer t.savePointMtx.Unlock()

	if t.savePoint > 0 {
		if debugSQL {
			println("ROLLBACK TO SAVEPOINT SP" + strconv.FormatUint(t.savePoint, 10))
		}
		_, err := t.tx.Exec("ROLLBACK TO SAVEPOINT SP" + strconv.FormatUint(t.savePoint, 10))
		if err != nil {
			return nil, err
		}

		t.savePoint--

		return ctx, nil
	}

	if debugSQL {
		println("ROLLBACK")
	}
	ctx = timelog.Start(ctx, "ROLLBACK")
	err := t.tx.Rollback()
	ctx = timelog.Finish(ctx)
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, s.transactionKey(), nil), nil
}

func (s *MySQL) DoInTransaction(ctx context.Context, f func(ctx context.Context) error) error {
	ctx, err := s.StartTransaction(ctx)
	if err != nil {
		return err
	}

	if err := f(ctx); err != nil {
		s.Rollback(ctx)
		return err
	}

	if _, err := s.Commit(ctx); err != nil {
		s.Rollback(ctx)
		return err
	}

	return nil
}

func (s *MySQL) GetTransaction(ctx context.Context) *sql.Tx {
	t := ctx.Value(s.transactionKey())

	if t == nil {
		return nil
	}

	return t.(*transaction).tx
}

func (s *MySQL) transactionKey() string {
	return ctx_transaction_key + strconv.FormatInt(int64(uintptr(unsafe.Pointer(s))), 10)
}
