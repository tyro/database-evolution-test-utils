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

import java.sql.Connection;

public class Assertions {

    protected Assertions() {
    }

    public static SchemaAssert assertThatSchema(Database schema, Connection connection) {
        return new SchemaAssert(schema, connection);
    }

    public static DataAssert assertThatTable(String tableName, Connection connection) {
        return new DataAssert(tableName, connection);
    }

    public static PrivilegeAssert assertThatUser(String username, String host, Connection connection) {
        return new PrivilegeAssert(username, host, connection);
    }
}
