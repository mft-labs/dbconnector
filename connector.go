package dbconnector

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
	"time"
)

type DbConnector struct {
	Connections []*sql.DB
	Urls        []string
}

func InitDb(url string) *sql.DB {
	con, err := sql.Open("postgres", url)
	if err != nil {
		return nil
	}
	return con
}

func (con *DbConnector) Init(urls string) *DbConnector {
	con.Urls = strings.Split(urls, ",")
	for _, url := range con.Urls {
		connection := InitDb(url)
		con.Connections = append(con.Connections, connection)
	}
	return con
}

type any = interface{}

func (con *DbConnector) Exec(statement string, params ...any) (sql.Result, error) {
	success := false
	index := 0
	var err error
	for {
		conn := con.Connections[index]
		err = conn.Ping()
		if err != nil {
			index = index + 1
			if index >= len(con.Connections) {
				index = 0
			}
			time.Sleep(time.Second)
			continue
		}
		result, err := conn.Exec(statement, params...)
		if err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || strings.Contains(err.Error(), "read: connection reset by peer") {
				index = index + 1
				time.Sleep(time.Second)
				if index >= len(con.Connections) {
					index = 0
				}
				continue
			} else {
				fmt.Printf("Error in exec %v\n", err)
				success = false
				break
			}
		} else {
			success = true
			return result, nil
		}
		if success {
			break
		}
	}
	if success == false {
		return nil, fmt.Errorf("Error occurred while executing %v:\n", statement)
	}
	return nil, nil
}

func (con *DbConnector) Query(statement string, params ...any) (*sql.Rows, error) {
	success := false
	index := 0
	var err error
	for {
		conn := con.Connections[index]
		err = conn.Ping()
		if err != nil {
			index = index + 1
			if index >= len(con.Connections) {
				index = 0
			}
			time.Sleep(time.Second)
			continue
		}
		rows, err := conn.Query(statement, params...)
		if err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || strings.Contains(err.Error(), "read: connection reset by peer") {
				index = index + 1
				time.Sleep(time.Second)
				if index >= len(con.Connections) {
					index = 0
				}
				continue
			} else {
				success = false
				break
			}
		} else {
			success = true
			return rows, nil
		}
	}
	if !success {
		return nil, fmt.Errorf("Error occurred while executing query %v:\n", statement)
	}
	return nil, nil
}
func (con *DbConnector) Prepare(statement string) (*sql.Stmt, error) {
	success := false
	index := 0
	var err error
	for {
		conn := con.Connections[index]
		err = conn.Ping()
		if err != nil {
			index = index + 1
			if index >= len(con.Connections) {
				index = 0
			}
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || strings.Contains(err.Error(), "read: connection reset by peer") {
				index = index + 1
				time.Sleep(time.Second)
				if index >= len(con.Connections) {
					index = 0
				}
				continue
			} else {
				success = false
				break
			}
		}
		stmt, err := conn.Prepare(statement)
		if err != nil {
			return nil, err
		}
		return stmt, nil
	}
	if !success {
		return nil, fmt.Errorf("Error occurred while preparing query %v:\n", statement)
	}
	return nil, nil
}
func (con *DbConnector) QueryRow(statement string, params ...any) *sql.Row {
	success := false
	index := 0
	var err error
	for {
		conn := con.Connections[index]
		err = conn.Ping()
		if err != nil {
			index = index + 1
			if index >= len(con.Connections) {
				index = 0
			}
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || strings.Contains(err.Error(), "read: connection reset by peer") {
				index = index + 1
				time.Sleep(time.Second)
				if index >= len(con.Connections) {
					index = 0
				}
				continue
			} else {
				success = false
				break
			}
		}
		row := conn.QueryRow(statement, params...)
		return row
	}
	if !success {
		return nil
	}
	return nil
}

func (con *DbConnector) QueryRowContext(ctx context.Context, statement string, params ...any) *sql.Row {
	success := false
	index := 0
	var err error
	for {
		conn := con.Connections[index]
		err = conn.Ping()
		if err != nil {
			index = index + 1
			if index >= len(con.Connections) {
				index = 0
			}
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), "write: broken pipe") || strings.Contains(err.Error(), "read: connection reset by peer") {
				index = index + 1
				time.Sleep(time.Second)
				if index >= len(con.Connections) {
					index = 0
				}
				continue
			} else {
				success = false
				break
			}
		}
		row := conn.QueryRowContext(ctx, statement, params...)
		return row
	}
	if !success {
		return nil
	}
	return nil
}
