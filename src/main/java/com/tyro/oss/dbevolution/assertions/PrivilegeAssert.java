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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class PrivilegeAssert {

    private final String username;
    private final String host;
    private final Connection connection;

    public PrivilegeAssert(String username, String host, Connection connection) {
        this.username = username;
        this.host = host;
        this.connection = connection;
    }

    public PrivilegeAssert hasPrivilege(String schemaName, String tableName, EnumSet<TablePrivilege> expectedPrivileges) {
        Set<TablePrivilege> tablePrivileges = new HashSet<>();
        tablePrivileges.addAll(getDatabaseTablePrivileges(schemaName, username, host));
        tablePrivileges.addAll(getTablePrivileges(schemaName, tableName, username, host));

        List<String> missingPrivileges = expectedPrivileges.stream()
                .filter(expectedPrivilege -> !tablePrivileges.contains(expectedPrivilege))
                .map(Enum::name)
                .collect(toList());

        assertThat(format("These privileges: %s should be granted to user: %s for table: %s.", missingPrivileges, username, tableName), missingPrivileges.isEmpty(), is(true));

        List<String> unexpectedPrivileges = tablePrivileges.stream()
                .filter(tablePrivilege -> !expectedPrivileges.contains(tablePrivilege))
                .map(Enum::name)
                .collect(toList());

        assertThat(format("These privileges: %s should not be granted to user: %s for table: %s.", unexpectedPrivileges, username, tableName), unexpectedPrivileges.isEmpty(), is(true));

        return this;
    }

    private List<TablePrivilege> getTablePrivileges(String schemaName, String tableName, String username, String host) {
        List<TablePrivilege> actualPrivileges = new ArrayList<>();

        PreparedStatement preparedStatement = null;
        ResultSet tablePrivileges = null;
        try {
            preparedStatement = connection.prepareStatement("SELECT table_priv FROM mysql.tables_priv where db = ? and user = ? and host = ? and table_name = ?");
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, username);
            preparedStatement.setString(3, host);
            preparedStatement.setString(4, tableName);

            tablePrivileges = preparedStatement.executeQuery();
            while (tablePrivileges.next()) {
                String[] table_privs = tablePrivileges.getString("table_priv").toUpperCase().split(",");
                for (String table_priv : table_privs) {
                    actualPrivileges.add(TablePrivilege.valueOf(table_priv.toUpperCase()));
                }
            }
        } catch (SQLException e) {
            fail(format("Unable to retrieve table permissions for user: %s and table: %s. Error message: %s", username, tableName, e.getMessage()));
        } finally {
            try {
                if (tablePrivileges != null) {
                    tablePrivileges.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException ignored) {
            }
        }

        return actualPrivileges;
    }

    private List<TablePrivilege> getDatabaseTablePrivileges(String schemaName, String username, String host) {
        List<TablePrivilege> actualPrivileges = new ArrayList<>();

        PreparedStatement preparedStatement = null;
        ResultSet tablePrivileges = null;

        try {
            preparedStatement = connection.prepareStatement("select select_priv, insert_priv, update_priv, delete_priv from mysql.db where Db = ? and USER = ? and HOST = ?");
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, username);
            preparedStatement.setString(3, host);

            tablePrivileges = preparedStatement.executeQuery();
            while (tablePrivileges.next()) {
                if (tablePrivileges.getString("select_priv").equals("Y")) {
                    actualPrivileges.add(TablePrivilege.SELECT);
                }
                if (tablePrivileges.getString("insert_priv").equals("Y")) {
                    actualPrivileges.add(TablePrivilege.INSERT);
                }
                if (tablePrivileges.getString("update_priv").equals("Y")) {
                    actualPrivileges.add(TablePrivilege.UPDATE);
                }
                if (tablePrivileges.getString("delete_priv").equals("Y")) {
                    actualPrivileges.add(TablePrivilege.DELETE);
                }
            }
        } catch (SQLException e) {
            fail(format("Unable to retrieve database level table permissions for user: %s. Error message: %s", username, e.getMessage()));
        } finally {
            try {
                if (tablePrivileges != null) {
                    tablePrivileges.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (SQLException ignored) {
            }
        }

        return actualPrivileges;
    }

    public enum TablePrivilege {
        SELECT, INSERT, UPDATE, DELETE
    }
}
