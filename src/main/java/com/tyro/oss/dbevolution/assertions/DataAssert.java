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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;
import static org.junit.jupiter.api.Assertions.*;

public class DataAssert {

    private final String tableName;
    private final Connection connection;

    public DataAssert(String tableName, Connection connection) {
        this.tableName = tableName;
        this.connection = connection;
    }

    public DataAssert isEmpty() throws SQLException {
        PreparedStatement statement = connection.prepareStatement("select count(*) from " + tableName);
        ResultSet result = statement.executeQuery();
        result.next();
        assertEquals(0, result.getInt(1), format("Number of rows in %s", tableName));
        return this;
    }

    public DataAssert hasRow(String columnName, String valueToMatch) throws SQLException {
        PreparedStatement statement;
        if (valueToMatch == null) {
            statement = connection.prepareStatement("select * from " + tableName + " where " + columnName + " IS NULL");
        } else {
            statement = connection.prepareStatement("select * from " + tableName + " where " + columnName + " = ?");
            statement.setObject(1, valueToMatch);
        }
        ResultSet results = statement.executeQuery();
        assertTrue(results.next());

        return this;
    }

    public DataAssert hasRowWithValues(Map<String, Object> columnValues) throws SQLException {
        ResultSet results = selectRowsWithColumnValues(tableName, columnValues);
        assertTrue(results.next());
        return this;
    }

    public DataAssert doesNotHaveRowWithValues(Map<String, Object> columnValues) throws SQLException {
        ResultSet results = selectRowsWithColumnValues(tableName, columnValues);
        assertFalse(results.next());
        return this;
    }

    public DataAssert doesNotHaveRowWithValue(String columnName, String valueToMatch) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("select * from " + tableName + " where " + columnName + " = ?");
        statement.setObject(1, valueToMatch);
        ResultSet results = statement.executeQuery();
        assertFalse(results.next());
        return this;
    }

    public DataAssert hasRowCount(int expectedRowCount) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("select count(*) from " + tableName)) {
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            long actualRowCount = rs.getLong(1);
            assertEquals(expectedRowCount, actualRowCount, format("# of rows in %s", tableName));
            assertFalse(rs.next());
        }

        return this;
    }

    public DataAssert hasRowsMatching(String tableA, String tableB, List<String> columnNames) {
        List<Row> rowsForTableA = getRowsForTable(tableA, columnNames, connection);
        List<Row> rowsForTableB = getRowsForTable(tableB, columnNames, connection);

        assertEquals(rowsForTableA.size(), rowsForTableB.size(), format("%s does not have the same number of rows as %s", tableA, tableB));
        assertEquals(rowsForTableA, rowsForTableB);

        return this;
    }

    public DataAssert hasColumnsMatching(String[] columnA, String[] columnB) throws SQLException {
        assertEquals(columnA.length, columnB.length);

        PreparedStatement preparedStatement = connection.prepareStatement(buildColumnComparisonSelectStatement(tableName, columnA, columnB));
        ResultSet rs = preparedStatement.executeQuery();

        assertTrue(rs.next());
        do {
            for (int i = 1; i < columnA.length + 1; i++) {
                assertTrue(rs.getBoolean(i),
                        format("column %s didn't match %s", columnA[i - 1], columnB[i - 1]));
            }
        } while (rs.next());

        return this;
    }

    private List<Row> getRowsForTable(String table, List<String> columns, Connection connection) {
        List<Row> rowData = new ArrayList<>();
        try {
            ResultSet results = connection.createStatement().executeQuery(format("select %s from %s order by id desc", join(columns.iterator(), ","), table));
            while (results.next()) {
                rowData.add(new Row(results, columns.size()));
            }
            return rowData;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSet selectRowsWithColumnValues(String tableName, Map<String, Object> columnValues) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * from ");
        sql.append(tableName).append(" where ");
        Iterator<Map.Entry<String, Object>> iterator = columnValues.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, Object> columnEntry = iterator.next();
            if (columnEntry.getValue() == null) {
                sql.append(columnEntry.getKey()).append(" IS NULL");
            } else {
                sql.append(columnEntry.getKey()).append(" = ?");
            }
            if (iterator.hasNext()) {
                sql.append(" AND ");
            }
        }

        int index = 1;
        PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());
        for (Map.Entry<String, Object> columnEntry : columnValues.entrySet()) {
            if (columnEntry.getValue() != null) {
                preparedStatement.setObject(index++, columnEntry.getValue());
            }
        }

        return preparedStatement.executeQuery();
    }

    private String buildColumnComparisonSelectStatement(String tableName, String[] newColumns, String[] oldColumns) {
        StringBuilder sqlStatement = new StringBuilder("Select ");
        for (int i = 0; i < newColumns.length; i++) {
            sqlStatement.append(newColumns[i]);
            sqlStatement.append(" = ");
            sqlStatement.append(oldColumns[i]);
            if (i != newColumns.length - 1) {
                sqlStatement.append(", ");
            }
        }
        sqlStatement.append(" from ").append(tableName);
        return sqlStatement.toString();
    }

    private static class Row {

        private final List<Object> data = new ArrayList<>();

        Row(ResultSet results, int numColumns) {
            try {
                for (int i = 1; i <= numColumns; i++) {
                    data.add(results.getObject(i));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }

        @Override
        public boolean equals(Object other) {
            return EqualsBuilder.reflectionEquals(this, other);
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

}
