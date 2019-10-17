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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ViewAssert {

    private final String viewName;
    private Map<String, Column> columns;

    public ViewAssert(DatabaseMetaData databaseMetadata, Database schema, String viewName) throws SQLException {
        this.viewName = viewName;
        ResultSet tables = databaseMetadata.getTables(null, schema.getName(), viewName, new String[]{"VIEW"});
        if (tables.next()) {
            this.columns = new HashMap<>();
            ResultSet columns = databaseMetadata.getColumns(null, schema.getName(), viewName, null);
            while (columns.next()) {
                try {
                    String name = columns.getString("COLUMN_NAME");
                    int type = columns.getInt("DATA_TYPE");
                    int size = columns.getInt("COLUMN_SIZE");
                    int scale = columns.getInt("DECIMAL_DIGITS");
                    this.columns.put(name, new Column(name, type, size, scale));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public ViewAssert isPresent() {
        assertNotNull(columns,
                format("View %s does not exist.", viewName));
        return this;
    }

    public void isNotPresent() {
        assertNull(columns,
                format("View %s should not exist.", viewName));
    }

    public ViewColumnAssert hasColumn(String columnName) {
        return new ViewColumnAssert(columnName, columns.get(columnName)).isPresent();
    }

    public ViewAssert doesNotHaveColumn(String columnName) {
        new ViewColumnAssert(columnName, columns.get(columnName)).isNotPresent();
        return this;
    }

    private static class Column {

        private final String name;
        private final int type;
        private final int size;
        private final int scale;

        public Column(String name, int type, int size, int scale) {
            this.name = name;
            this.type = type;
            this.size = size;
            this.scale = scale;
        }
    }

    public class ViewColumnAssert {

        private final String columnName;
        private final Column columns;

        public ViewColumnAssert(String columnName, Column columns) {
            this.columnName = columnName;
            this.columns = columns;
        }

        public ViewColumnAssert isPresent() {
            assertNotNull(columns,
                    format("Column %s does not exist.", columnName));
            return this;
        }

        public void isNotPresent() {
            assertNull(columns,
                    format("Column %s should not exist.", columnName));
        }

        public ViewColumnAssert hasColumn(String columnName) {
            return ViewAssert.this.hasColumn(columnName);
        }
    }
}
