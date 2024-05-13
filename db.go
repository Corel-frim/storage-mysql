package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/go-qbit/model"
	"github.com/go-qbit/qerror"
	"github.com/go-qbit/timelog"
)

var (
	debugSQL  = os.Getenv("MYSQL_DEBUG") != ""
	SqlDriver = "mysql"
)

type MySQL struct {
	db        *sql.DB
	models    map[string]model.IModel
	modelsMtx sync.RWMutex
}

func NewMySQL() *MySQL {
	s := &MySQL{
		models: make(map[string]model.IModel),
	}

	return s
}

func (s *MySQL) Connect(dsn string) error {
	db, err := sql.Open(SqlDriver, dsn)
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

func (s *MySQL) SetupConnectionsPool(maxOpenConns, maxIdleConns int, maxLifetime time.Duration) {
	if maxOpenConns > 0 {
		s.db.SetMaxOpenConns(maxOpenConns)
	}
	if maxIdleConns > 0 {
		s.db.SetMaxIdleConns(maxIdleConns)
	}
	if maxLifetime > 0 {
		s.db.SetConnMaxLifetime(maxLifetime)
	}
}

func (s *MySQL) GetRawDB() *sql.DB {
	return s.db
}

func (s *MySQL) Disconnect() error {
	return s.db.Close()
}

func (s *MySQL) NewModel(id string, fields []model.IFieldDefinition, opts model.BaseModelOpts) model.IModel {
	mysqlFields := make([]IMysqlFieldDefinition, len(fields))
	for i, _ := range fields {
		mysqlFields[i] = fields[i].(IMysqlFieldDefinition)
	}

	return NewBaseModel(s, id, mysqlFields, nil, BaseModelOpts{BaseModelOpts: opts})
}

func (s *MySQL) RegisterModel(m model.IModel) error {
	s.modelsMtx.Lock()
	defer s.modelsMtx.Unlock()

	if _, exists := s.models[m.GetId()]; exists {
		return qerror.Errorf("Model '%s' is already exists", m.GetId())
	}

	s.models[m.GetId()] = m

	return nil
}

func (s *MySQL) WriteCreateSQL(sqlBuf *SqlBuffer) {
	modelLevels := s.getModelsLevels()

	sort.Sort(modelLevels)

	s.modelsMtx.RLock()
	defer s.modelsMtx.RUnlock()

	for _, modelLevel := range modelLevels {
		s.models[modelLevel.name].(*BaseModel).WriteCreateSQL(sqlBuf)
		sqlBuf.WriteString(";\n")
	}
}

func (s *MySQL) InitDB(ctx context.Context) error {
	modelLevels := s.getModelsLevels()
	sort.Sort(modelLevels)

	sqlBuf := NewSqlBuffer()

	s.modelsMtx.RLock()
	defer s.modelsMtx.RUnlock()

	for _, modelLevel := range modelLevels {
		sqlBuf.Reset()
		s.models[modelLevel.name].(*BaseModel).WriteCreateSQL(sqlBuf)
		if _, err := s.Exec(ctx, sqlBuf.GetSQL(), sqlBuf.GetArgs()...); err != nil {
			return err
		}
	}

	return nil
}

func (s *MySQL) Exec(ctx context.Context, sql string, a ...interface{}) (driver.Result, error) {
	sqlBuf := &SqlBuffer{Buffer: bytes.NewBufferString(sql), args: a}

	ctx = timelog.Start(ctx, sqlBuf)
	defer timelog.Finish(ctx)

	ct := ctx.Value(s.transactionKey())

	var (
		res driver.Result
		err error
	)

	if debugSQL {
		println(sqlBuf.String())
	}

	if ct == nil {
		res, err = s.db.Exec(sql, a...)
	} else {
		res, err = ct.(*transaction).tx.Exec(sql, a...)
	}

	return res, err
}

func (s *MySQL) RawQuery(ctx context.Context, query string, a ...interface{}) (*sql.Rows, error) {
	sqlBuf := &SqlBuffer{Buffer: bytes.NewBufferString(query), args: a}

	ctx = timelog.Start(ctx, sqlBuf)
	defer timelog.Finish(ctx)

	ct := ctx.Value(s.transactionKey())

	var (
		res *sql.Rows
		err error
	)

	if debugSQL {
		println(sqlBuf.String())
	}

	if ct == nil {
		res, err = s.db.Query(query, a...)
	} else {
		res, err = ct.(*transaction).tx.Query(query, a...)
	}

	return res, err
}

func (s *MySQL) Add(ctx context.Context, m model.IModel, data *model.Data, opts model.AddOptions) (*model.Data, error) {
	sqlBuf := NewSqlBuffer()

	sqlBuf.WriteString("INSERT INTO ")
	sqlBuf.WriteIdentifier(m.GetId())

	sqlBuf.WriteByte('(')
	sqlBuf.WriteIdentifiersList(data.Fields())
	sqlBuf.WriteByte(')')

	sqlBuf.WriteString("VALUES")

	for i, row := range data.Data() {
		if i != 0 {
			sqlBuf.WriteByte(',')
		}
		sqlBuf.WriteByte('(')
		sqlBuf.WriteValuesList(row)
		sqlBuf.WriteByte(')')
	}

	if opts.Replace {
		sqlBuf.WriteString("ON DUPLICATE KEY UPDATE ")
		for i, fieldName := range data.Fields() {
			if i > 0 {
				sqlBuf.WriteByte(',')
			}
			sqlBuf.WriteIdentifier(fieldName)
			sqlBuf.WriteString("=VALUES(")
			sqlBuf.WriteIdentifier(fieldName)
			sqlBuf.WriteByte(')')
		}
	}

	execRes, err := s.Exec(ctx, sqlBuf.GetSQL(), sqlBuf.GetArgs()...)
	if err != nil {
		return nil, err
	}

	pKFieldsNames := m.GetPKFieldsNames()
	fieldsPos := make(map[string]int)
	for i, fieldName := range data.Fields() {
		fieldsPos[fieldName] = i
	}

	lastInsertId, _ := execRes.LastInsertId()

	res := make([][]interface{}, data.Len())
	for i, row := range data.Data() {
		rowRes := make([]interface{}, len(pKFieldsNames))
		for j, fieldName := range pKFieldsNames {
			fp, exists := fieldsPos[fieldName]
			if exists {
				if rv := reflect.ValueOf(row[fp]); rv.Kind() == reflect.Ptr && !rv.IsNil() {
					rowRes[j] = rv.Elem().Interface()
				} else {
					rowRes[j] = row[fp]
				}
			}
			if (!exists || isNil(rowRes[j])) && m.GetFieldDefinition(fieldName).(IMysqlFieldDefinition).IsAutoIncremented() {
				switch m.GetFieldDefinition(fieldName).GetType().Kind() {
				case reflect.Int8:
					rowRes[j] = int8(lastInsertId)
				case reflect.Int16:
					rowRes[j] = int16(lastInsertId)
				case reflect.Int32:
					rowRes[j] = int32(lastInsertId)
				case reflect.Int64:
					rowRes[j] = int64(lastInsertId)
				case reflect.Uint8:
					rowRes[j] = uint8(lastInsertId)
				case reflect.Uint16:
					rowRes[j] = uint16(lastInsertId)
				case reflect.Uint32:
					rowRes[j] = uint32(lastInsertId)
				case reflect.Uint64:
					rowRes[j] = uint64(lastInsertId)
				default:
					panic("Not implemented")
				}
				lastInsertId++
			}
		}
		res[i] = rowRes
	}

	return model.NewData(m.GetPKFieldsNames(), res), nil
}

func (s *MySQL) Query(ctx context.Context, m model.IModel, fieldsNames []string, options model.GetAllOptions) (*model.Data, error) {
	sqlBuf := NewSqlBuffer()

	sqlBuf.WriteString("SELECT ")

	if options.Distinct {
		sqlBuf.WriteString(" DISTINCT ")
	}

	if options.RowsWoLimit != nil {
		sqlBuf.WriteString(" SQL_CALC_FOUND_ROWS ")
	}

	sqlBuf.WriteIdentifiersList(fieldsNames)
	sqlBuf.WriteString(" FROM ")
	sqlBuf.WriteIdentifier(m.GetId())

	if options.Filter != nil {
		sqlBuf.WriteString(" WHERE ")
		options.Filter.GetProcessor(exprProcessor).(WriteFunc)(sqlBuf)
	}

	if len(options.OrderBy) > 0 {
		sqlBuf.WriteString(" ORDER BY ")
		for i, order := range options.OrderBy {
			if i > 0 {
				sqlBuf.WriteString(",")
			}
			sqlBuf.WriteIdentifier(order.FieldName)
			if order.Desc {
				sqlBuf.WriteString(" DESC")
			}
		}
	}

	if options.Limit > 0 {
		sqlBuf.WriteString(" LIMIT ")
		sqlBuf.WriteString(strconv.FormatUint(options.Limit, 10))
		if options.Offset > 0 {
			sqlBuf.WriteString(" OFFSET ")
			sqlBuf.WriteString(strconv.FormatUint(options.Offset, 10))
		}
	}

	if options.ForUpdate {
		sqlBuf.WriteString(" FOR UPDATE")
	}

	if options.RowsWoLimit != nil {
		var err error
		if ctx, err = s.StartTransaction(ctx); err != nil { // For using 1 connection
			return nil, err
		}
	}
	rows, err := s.RawQuery(ctx, sqlBuf.GetSQL(), sqlBuf.GetArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnsNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	res := model.NewEmptyData(columnsNames)

	for rows.Next() {
		rawRow := make([]interface{}, len(columnsNames))
		for i, name := range columnsNames {
			field := m.GetFieldDefinition(name)

			rawRow[i] = reflect.New(field.GetType()).Interface()
		}

		err := rows.Scan(rawRow...)
		if err != nil {
			return nil, err
		}

		row := make([]interface{}, len(columnsNames))
		for i, _ := range columnsNames {
			row[i] = reflect.ValueOf(rawRow[i]).Elem().Interface()
		}
		res.Add(row)
	}

	if options.RowsWoLimit != nil {
		rows, err := s.RawQuery(ctx, "SELECT FOUND_ROWS()")
		if err != nil {
			return nil, err
		}

		rows.Next()
		rows.Scan(options.RowsWoLimit)
		rows.Next()

		ctx, err = s.Commit(ctx)
		if err != nil {
			return nil, err
		}
	}

	return res, nil
}

func (s *MySQL) Edit(ctx context.Context, m model.IModel, filter model.IExpression, newValues map[string]interface{}) error {
	sqlBuf := NewSqlBuffer()

	sqlBuf.WriteString("UPDATE ")
	sqlBuf.WriteIdentifier(m.GetId())
	sqlBuf.WriteString(" SET ")
	first := true
	for name, value := range newValues {
		if first {
			first = false
		} else {
			sqlBuf.WriteString(", ")
		}
		sqlBuf.WriteIdentifier(name)
		sqlBuf.WriteByte('=')
		sqlBuf.WriteValue(value)
	}

	if filter != nil {
		sqlBuf.WriteString(" WHERE ")
		filter.GetProcessor(exprProcessor).(WriteFunc)(sqlBuf)
	}

	_, err := s.Exec(ctx, sqlBuf.GetSQL(), sqlBuf.GetArgs()...)

	return err
}

func (s *MySQL) Delete(ctx context.Context, m model.IModel, filter model.IExpression) error {
	sqlBuf := NewSqlBuffer()

	sqlBuf.WriteString("DELETE FROM ")
	sqlBuf.WriteIdentifier(m.GetId())

	if filter != nil {
		sqlBuf.WriteString(" WHERE ")
		filter.GetProcessor(exprProcessor).(WriteFunc)(sqlBuf)
	}

	_, err := s.Exec(ctx, sqlBuf.GetSQL(), sqlBuf.GetArgs()...)

	return err
}

func QuoteIdentifier(identifier string) string {
	return "`" + strings.Replace(identifier, "`", "``", -1) + "`"
}

func Quote(value interface{}) string {
	// FixMe: Replace termporary solution
	var v string

	switch value := value.(type) {
	case nil:
		v = "NULL"
	case []byte:
		v = "X`"
		if len(value) <= 1024 {
			for _, b := range value {
				v += strconv.FormatUint(uint64(b), 10)
			}
		} else {
			v += fmt.Sprintf("DATA(%d)", len(value))
		}
		v += "'"
	case string:
		v = "'" + strings.Replace(value, "'", "''", -1) + "'"
	case int:
		v = strconv.FormatInt(int64(value), 10)
	case int8:
		v = strconv.FormatInt(int64(value), 10)
	case int16:
		v = strconv.FormatInt(int64(value), 10)
	case int32:
		v = strconv.FormatInt(int64(value), 10)
	case int64:
		v = strconv.FormatInt(value, 10)
	case uint:
		v = strconv.FormatUint(uint64(value), 10)
	case uint8:
		v = strconv.FormatUint(uint64(value), 10)
	case uint16:
		v = strconv.FormatUint(uint64(value), 10)
	case uint32:
		v = strconv.FormatUint(uint64(value), 10)
	case uint64:
		v = strconv.FormatUint(value, 10)
	case float32:
		v = strconv.FormatFloat(float64(value), 'f', -1, 32)
	case float64:
		v = strconv.FormatFloat(value, 'f', -1, 64)
	case bool:
		if value {
			v = "TRUE"
		} else {
			v = "FALSE"
		}
	case time.Time:
		v = value.Format("2006-01-02 15:04:05")

	case *string:
		if value == nil {
			v = "NULL"
		} else {
			v = "'" + strings.Replace(*value, "'", "''", -1) + "'"
		}
	case *int:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatInt(int64(*value), 10)
		}
	case *int8:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatInt(int64(*value), 10)
		}
	case *int16:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatInt(int64(*value), 10)
		}
	case *int32:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatInt(int64(*value), 10)
		}
	case *int64:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatInt(*value, 10)
		}
	case *uint:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatUint(uint64(*value), 10)
		}
	case *uint8:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatUint(uint64(*value), 10)
		}
	case *uint16:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatUint(uint64(*value), 10)
		}
	case *uint32:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatUint(uint64(*value), 10)
		}
	case *uint64:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatUint(*value, 10)
		}
	case *float32:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatFloat(float64(*value), 'f', -1, 32)
		}
	case *float64:
		if value == nil {
			v = "NULL"
		} else {
			v = strconv.FormatFloat(*value, 'f', -1, 64)
		}
	case *bool:
		if value == nil {
			v = "NULL"
		} else {
			if *value {
				v = "TRUE"
			} else {
				v = "FALSE"
			}
		}
	case *time.Time:
		if value == nil {
			v = "NULL"
		} else {
			v = value.Format("2006-01-02 15:04:05")
		}

	default:
		panic(fmt.Sprintf("%T is not implemented", value))
	}

	return v
}

func isNil(v interface{}) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)

	return rv.Kind() == reflect.Ptr && rv.IsNil()
}
