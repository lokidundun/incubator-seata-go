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
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"

	"seata.apache.org/seata-go/pkg/datasource/sql/types"
)

type seataATConnector struct {
	*SeataConnector
	transType types.TransactionMode
}

func (c *seataATConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.SeataConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	_conn, _ := conn.(*Conn)

	return &ATConn{
		Conn: _conn,
	}, nil
}

func (c *seataATConnector) Driver() driver.Driver {
	return &seataATDriver{
		SeataDriver: c.SeataConnector.Driver().(*SeataDriver),
	}
}

type seataXAConnector struct {
	*SeataConnector
	transType types.TransactionMode
}

func (c *seataXAConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.SeataConnector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	_conn, _ := conn.(*Conn)

	return &XAConn{
		Conn: _conn,
	}, nil
}

func (c *seataXAConnector) Driver() driver.Driver {
	return &seataXADriver{
		SeataDriver: c.SeataConnector.Driver().(*SeataDriver),
	}
}

// Connect returns a connection to the database.
func (c *SeataConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.target.Connect(ctx)
	if err != nil {
		return nil, err
	}

	// Get dbName and dbType from the resource
	dbName := c.Res.dbName
	dbType := c.Res.dbType

	// For MySQL, also check cfg for dbName
	if dbName == "" && c.Cfg != nil {
		if mysqlCfg, ok := c.Cfg.(*mysql.Config); ok {
			dbName = mysqlCfg.DBName
		}
	}

	return &Conn{
		targetConn: conn,
		res:        c.Res,
		txCtx:      types.NewTxCtx(),
		autoCommit: true,
		dbName:     dbName,
		dbType:     dbType,
	}, nil
}

// Driver returns the underlying Driver of the Connector
func (c *SeataConnector) Driver() driver.Driver {
	c.once.Do(func() {
		d := c.target.Driver()
		c.driver = d
	})

	return &SeataDriver{
		Target: c.driver,
	}
}

// GetResource returns the DBResource
func (c *SeataConnector) GetResource() *DBResource {
	return c.Res
}

// GetConfig returns the database-specific config
func (c *SeataConnector) GetConfig() interface{} {
	return c.Cfg
}
