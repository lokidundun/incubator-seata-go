/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"

	"seata.apache.org/seata-go/pkg/datasource/sql/datasource"
	mysql2 "seata.apache.org/seata-go/pkg/datasource/sql/datasource/mysql"
	"seata.apache.org/seata-go/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/pkg/datasource/sql/util"
	"seata.apache.org/seata-go/pkg/protocol/branch"
	"seata.apache.org/seata-go/pkg/util/log"
)

const (
	// SeataATMySQLDriver MySQL driver for AT mode
	SeataATMySQLDriver = "seata-at-mysql"
	// SeataXAMySQLDriver MySQL driver for XA mode
	SeataXAMySQLDriver = "seata-xa-mysql"
)

func initDriver() {
	sql.Register(SeataATMySQLDriver, &seataATDriver{
		SeataDriver: &SeataDriver{
			BranchType: branch.BranchTypeAT,
			TransType:  types.ATMode,
			Target:     mysql.MySQLDriver{},
		},
	})

	sql.Register(SeataXAMySQLDriver, &seataXADriver{
		SeataDriver: &SeataDriver{
			BranchType: branch.BranchTypeXA,
			TransType:  types.XAMode,
			Target:     mysql.MySQLDriver{},
		},
	})
}

type seataATDriver struct {
	*SeataDriver
}

func (d *seataATDriver) OpenConnector(name string) (c driver.Connector, err error) {
	connector, err := d.SeataDriver.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	_connector, _ := connector.(*SeataConnector)
	_connector.TransType = types.ATMode
	cfg, _ := mysql.ParseDSN(name)
	_connector.Cfg = cfg

	return &seataATConnector{
		SeataConnector: _connector,
	}, nil
}

type seataXADriver struct {
	*SeataDriver
}

func (d *seataXADriver) OpenConnector(name string) (c driver.Connector, err error) {
	connector, err := d.SeataDriver.OpenConnector(name)
	if err != nil {
		return nil, err
	}

	_connector, _ := connector.(*SeataConnector)
	_connector.TransType = types.XAMode
	cfg, _ := mysql.ParseDSN(name)
	_connector.Cfg = cfg

	return &seataXAConnector{
		SeataConnector: _connector,
	}, nil
}

// Open never be called, because SeataDriver implemented driver.DriverContext interface.
func (d *SeataDriver) Open(name string) (driver.Conn, error) {
	return nil, errors.New("operation unsupport")
}

func (d *SeataDriver) OpenConnector(name string) (c driver.Connector, err error) {
	c = &dsnConnector{dsn: name, driver: d.Target}
	if driverCtx, ok := d.Target.(driver.DriverContext); ok {
		c, err = driverCtx.OpenConnector(name)
		if err != nil {
			log.Errorf("open connector: %v", err)
			return nil, err
		}
	}

	dbType := types.ParseDBType(d.getTargetDriverName())
	if dbType == types.DBTypeUnknown {
		return nil, fmt.Errorf("unsupport conn type %s", d.getTargetDriverName())
	}

	proxy, err := d.getOpenConnectorProxy(c, dbType, sql.OpenDB(c), name)
	if err != nil {
		log.Errorf("register resource: %v", err)
		return nil, err
	}

	return proxy, nil
}

func (d *SeataDriver) getOpenConnectorProxy(connector driver.Connector, dbType types.DBType,
	db *sql.DB, dataSourceName string) (driver.Connector, error) {
	cfg, _ := mysql.ParseDSN(dataSourceName)
	options := []dbOption{
		WithResourceID(parseResourceID(dataSourceName)),
		WithTarget(db),
		WithBranchType(d.BranchType),
		WithDBType(dbType),
		WithDBName(cfg.DBName),
		WithConnector(connector),
	}
	res, err := NewResource(options...)
	if err != nil {
		log.Errorf("create new resource: %v", err)
		return nil, err
	}
	datasource.RegisterTableCache(types.DBTypeMySQL, mysql2.NewTableMetaInstance(db, cfg))
	if err = datasource.GetDataSourceManager(d.BranchType).RegisterResource(res); err != nil {
		log.Errorf("register resource: %v", err)
		return nil, err
	}
	return NewSeataConnector(connector, res, cfg), nil
}

func (d *SeataDriver) getTargetDriverName() string {
	return "mysql"
}

// NewResource creates a new DBResource
func NewResource(opts ...dbOption) (*DBResource, error) {
	db := new(DBResource)

	for i := range opts {
		opts[i](db)
	}

	db.init()
	return db, nil
}

type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (t *dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dsn)
}

func (t *dsnConnector) Driver() driver.Driver {
	return t.driver
}

func parseResourceID(dsn string) string {
	i := strings.Index(dsn, "?")
	res := dsn
	if i > 0 {
		res = dsn[:i]
	}
	return strings.ReplaceAll(res, ",", "|")
}

func selectDBVersion(ctx context.Context, conn driver.Conn) (string, error) {
	var rowsi driver.Rows
	var err error

	queryerCtx, ok := conn.(driver.QueryerContext)
	var queryer driver.Queryer
	if !ok {
		queryer, ok = conn.(driver.Queryer)
	}
	if ok {
		rowsi, err = util.CtxDriverQuery(ctx, queryerCtx, queryer, "SELECT VERSION()", nil)
		defer func() {
			if rowsi != nil {
				rowsi.Close()
			}
		}()
		if err != nil {
			log.Errorf("ctx driver query: %+v", err)
			return "", err
		}
	} else {
		log.Errorf("target conn should been driver.QueryerContext or driver.Queryer")
		return "", fmt.Errorf("invalid conn")
	}

	dest := make([]driver.Value, 1)
	var version string
	if err = rowsi.Next(dest); err != nil {
		if err == io.EOF {
			return version, nil
		}
		return "", err
	}
	if len(dest) != 1 {
		return "", errors.New("get db version is not column 1")
	}

	switch reflect.TypeOf(dest[0]).Kind() {
	case reflect.Slice, reflect.Array:
		val := reflect.ValueOf(dest[0]).Bytes()
		version = string(val)
	case reflect.String:
		version = reflect.ValueOf(dest[0]).String()
	default:
		return "", errors.New("get db version is not a string")
	}

	return version, nil
}
