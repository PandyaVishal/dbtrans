/*MIT License

Copyright (c) 2017 Vishal Pandya

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package dbtrans

import (
	"database/sql"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"vlog"
	"strings"
	"sync"
)

//This DT(Database Transactions) is what will be used by the caller.
type DT struct {
	driver string
}

//Structure to return query(select) results
type Rows struct {
	Colname string
	Colvals []string
}

type DBInterfacer interface {
	Exec(string, ...interface{}) (int64, error)
	QueryFetch(string, ...interface{}) ([]Rows, error)
}

//Globals
var (
	dbmap map[string]*sql.DB
	mu    sync.RWMutex
)

//Called during package initialization
func init() {
	dbmap = make(map[string]*sql.DB)
	mu = sync.RWMutex{}
}

//The drivername parameter contains the name of the database driver
//and the dsn is the connection string to that database.
//It's a safe call.
func Open(drivername string, dsn string, cnum int) (*DT, error) {
	dt := &DT{drivername}
	mu.RLock()
	c, ok := dbmap[drivername]
	mu.RUnlock()
	if ok {
		return dt, c.Ping()
	}
	c, err := sql.Open(drivername, dsn)
	if err != nil {
		log.Infod("Cannot open connection to the database\n")
		return nil, err
	}

	//The driver is registered
	mu.Lock()
	dbmap[drivername] = c
	mu.Unlock()
	//sql package takes care of pooling.
	c.SetMaxOpenConns(cnum)
	c.SetMaxIdleConns(cnum)
	return dt, c.Ping()
}

//For SQL Select operation.
//Query and Fetch results.
func (dt *DT) QueryFetch(sqlstmt string, args ...interface{}) ([]Rows, error) {
	var rs *sql.Rows
	mu.RLock()
	c, ok := dbmap[dt.driver]
	mu.RUnlock()
	if !ok {
		err := errors.New("Connection not open..")
		log.Infod(err)
		return nil, err
	}
	tx, err := c.Begin()
	if err != nil {
		log.Infod("Error in Begin Transaction")
		return nil, err
	}
	switch strings.Fields(sqlstmt)[0] {
	case "SELECT", "SEL", "select", "sel":
		switch args {
		case nil:
			rs, err = tx.Query(sqlstmt)
		default:
			rs, err = tx.Query(sqlstmt, args...)
		}
		if err != nil {
			log.Infod("Error executing the query(tx.Query):", sqlstmt, err)
			tx.Rollback()
			return nil, err
		}
		cols, err := rs.Columns()

		if err != nil {
			log.Infod("Error returning query metadata")
			return nil, err
		}
		cf := make([]Rows, len(cols))
		for m := 0; m < len(cols); m++ {
			cf[m].Colname = cols[m]
		}

		for rs.Next() {
			vals := make([]string, len(cols))
			v := make([]interface{}, len(cols))
			for i, _ := range vals {
				v[i] = &vals[i]
			}
			if err := rs.Scan(v...); err != nil {
				log.Infod("Error scanning rows:")
				return nil, err
			}
			for m := 0; m < len(cols); m++ {
				cf[m].Colvals = append(cf[m].Colvals, vals[m])
			}
		}
		//close the result set and release to pool
		if err := rs.Close(); err != nil {
			log.Infod("Error closing the result set")
			return nil, err
		}
		if err := rs.Err(); err != nil {
			log.Infod("Error iterating the rows")
			return nil, err
		}
		//Commit the transaction
		if err := tx.Commit(); err != nil {
			log.Infod("Error closing the result set")
			return nil, err
		}
		return cf, nil

	default:
		err := errors.New("Invalid method call, use DBExecFetch")
		log.Infod(err)
		return nil, err

	}
}

//For SQL statements which are not select.
func (dt *DT) Exec(sqlstmt string, args ...interface{}) (int64, error) {
	var ra sql.Result
	var numRows int64
	mu.RLock()
	c, ok := dbmap[dt.driver]
	mu.RUnlock()
	if !ok {
		err := errors.New("Connection not open..")
		log.Infod(err)
		return 0, err
	}
	tx, err := c.Begin()
	if err != nil {
		log.Infod("Error in Begin Transaction")
		return 0, err
	}
	switch strings.Fields(sqlstmt)[0] {
	case "SELECT", "SEL", "select", "sel":
		err := errors.New("Invalid method call on Select, use DBQueryFetch")
		log.Infod(err)
		return 0, err
	default:
		switch args {
		case nil:
			ra, err = tx.Exec(sqlstmt)
		default:
			ra, err = tx.Exec(sqlstmt, args...)
		}
		if err != nil {
			log.Infod("Error executing the query(tx.Exec):", sqlstmt, err)
			tx.Rollback()
			return 0, err
		}
		numRows, err = ra.RowsAffected()
		if err != nil {
			log.Infod("Error in fetching rows affected")
			return 0, err
		}
		if err = tx.Commit(); err != nil {
			log.Infod("Error closing the result set")
			return 0, err
		}
		return numRows, nil
	}
}
