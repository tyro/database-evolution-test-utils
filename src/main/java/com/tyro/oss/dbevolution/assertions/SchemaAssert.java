/*
 * Copyright 2019 Tyro Payments Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tyro.oss.dbevolution.assertions;

import org.apache.ddlutils.model.Database;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

public class SchemaAssert extends Assert {

    private final Database schema;
    private final DatabaseMetaData databaseMetadata;
    private final Connection connection;

    public SchemaAssert(Database schema, Connection connection) {
        this.schema = schema;
        this.connection = connection;
        try {
            this.databaseMetadata = this.connection.getMetaData();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public TableAssert doesNotHaveTable(String tableName) {
        return new TableAssert(schema, tableName).isNotPresent();
    }

    public SchemaAssert doesNotHaveView(String viewName) {
        try {
            new ViewAssert(databaseMetadata, schema, viewName).isNotPresent();
            return this;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public TableAssert hasTable(String tableName) {
        return new TableAssert(schema, tableName).isPresent();
    }

    public ViewAssert hasView(String viewName) {
        try {
            return new ViewAssert(databaseMetadata, schema, viewName).isPresent();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
