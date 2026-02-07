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
	"database/sql/driver"
	"sync"

	"seata.apache.org/seata-go/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/pkg/protocol/branch"
)

// SeataDriver is the base driver struct for Seata
type SeataDriver struct {
	BranchType branch.BranchType
	TransType  types.TransactionMode
	Target     driver.Driver
}

// SeataConnector represents a driver in a fixed configuration
type SeataConnector struct {
	TransType types.TransactionMode
	Res       *DBResource
	once      sync.Once
	driver    driver.Driver
	target    driver.Connector
	Cfg       interface{}
}

// NewSeataConnector creates a new SeataConnector
func NewSeataConnector(target driver.Connector, res *DBResource, cfg interface{}) *SeataConnector {
	return &SeataConnector{
		Res:   res,
		Cfg:   cfg,
	}
}
