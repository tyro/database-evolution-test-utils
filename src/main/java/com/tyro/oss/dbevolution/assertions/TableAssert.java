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

import org.apache.ddlutils.model.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

public class TableAssert {

    private final String name;
    private final Database schema;
    private boolean isNewTable;

    public TableAssert(Database schema, String tableName) {
        this.schema = schema;
        this.name = tableName;
    }

    public TableAssert isPresent() {
        assertTablePresent(schema, name);
        return this;
    }

    public TableAssert isNotPresent() {
        assertTableNotPresent(schema, name);
        return this;
    }

    private void assertTablePresent(Database database, String tableName) {
        assertNotNull("Table '" + database.getName() + "." + tableName + "' not present", database.findTable(tableName));
    }

    private void assertTableNotPresent(Database database, String tableName) {
        assertNull("Table " + tableName + " should not exist", database.findTable(tableName));
    }

    public ColumnAssert hasColumn(String columnName) {
        return new ColumnAssert(schema, this, columnName).isPresent();
    }

    public TableAssert doesNotHaveColumn(String columnName) {
        return new ColumnAssert(schema, this, columnName).isNotPresent().andTable();
    }

    public TableAssert enterNewTableAssertionMode() {
        this.isNewTable = true;
        return this;
    }

    public TableAssert hasIndexOn(String... columnNames) {
        return testForIndexOnColumns(true, columnNames);
    }

    public TableAssert hasNoIndexOn(String... columnNames) {
        return testForIndexOnColumns(false, columnNames);
    }

    public TableAssert hasNonUniqueIndexOn(String... columnNames) {
        return testForIndexOnColumns(true, false, columnNames);
    }

    public TableAssert hasUniqueIndexOn(String... columnNames) {
        return testForIndexOnColumns(true, true, columnNames);
    }

    public TableAssert hasNoIndexes() {
        assertEquals("Index count", 0, schema.findTable(name).getIndexCount());
        return this;
    }

    private Index indexOnColumns(String... columnNames) {
        Table table = schema.findTable(name);
        return stream(table.getIndices())
                .filter(index -> asList(columnNames).equals(getColumnNames(index)))
                .findFirst()
                .orElse(null);
    }

    private TableAssert testForIndexOnColumns(boolean wantToFindIndex, String... columnNames) {
        Index matchingIndex = indexOnColumns(columnNames);

        if (wantToFindIndex) {
            assertNotNull("No matching index found on column/s (" + String.join(",", columnNames) + ")", matchingIndex);
        } else {
            assertNull("Matching index found on column " + String.join(",", columnNames), matchingIndex);
        }
        return this;
    }

    private TableAssert testForIndexOnColumns(boolean wantToFindIndex, boolean isUniqueIndex, String... columnNames) {
        Index matchingIndex = indexOnColumns(columnNames);

        if (wantToFindIndex) {
            assertNotNull("No matching index found on column " + String.join(",", columnNames), matchingIndex);
            if (isUniqueIndex) {
                assertTrue("Matching index found but not unique", matchingIndex.isUnique());
            } else {
                assertFalse("Matching index found but it's specified as 'unique'", matchingIndex.isUnique());
            }
        } else {
            assertNull("Matching index found on column " + String.join(",", columnNames), matchingIndex);
        }
        return this;
    }

    private List<String> getColumnNames(Index index) {
        return stream(index.getColumns())
                .map(IndexColumn::getName)
                .collect(toList());
    }

    public TableAssert hasIndexNamed(String indexName) {
        return testForIndexByName(true, indexName);
    }

    public TableAssert hasNoIndexNamed(String indexName) {
        return testForIndexByName(false, indexName);
    }

    private TableAssert testForIndexByName(boolean wantToFindIndex, String indexName) {
        Table table = schema.findTable(name);
        Index matchingIndex = stream(table.getIndices())
                .filter(index -> index.getName().equals(indexName))
                .findFirst()
                .orElse(null);

        if (wantToFindIndex) {
            assertNotNull("Matching index found", matchingIndex);
        } else {
            assertNull("Matching index found", matchingIndex);
        }
        return this;
    }

    public TableAssert hasPrimaryKeyOn(String... expectedPrimaryKeyColumnNames) {
        Table table = schema.findTable(name);
        Set<String> actualPrimaryKeyColumnNames = new HashSet<>();
        for (Column column : table.getPrimaryKeyColumns()) {
            actualPrimaryKeyColumnNames.add(column.getName());
        }
        assertEquals(new HashSet<>(asList(expectedPrimaryKeyColumnNames)), actualPrimaryKeyColumnNames);

        return this;
    }

    public TableAssert hasForeignKeyOn(final String foreignTableName, final String localTableColumn, final String foreignTableColumn) {
        Table table = schema.findTable(name);
        ForeignKey[] foreignKeys = table.getForeignKeys();

        boolean matchFound = false;
        for (ForeignKey foreignKey : foreignKeys) {
            if (foreignKey.getForeignTableName().equals(foreignTableName)) {
                for (Reference ref : foreignKey.getReferences()) {
                    if (ref.getForeignColumnName().equals(foreignTableColumn) && ref.getLocalColumnName().equals(localTableColumn)) {
                        matchFound = true;
                    }
                }
            }
        }
        assertTrue(format("Foreign Key from column %s to table %s column %s does not exit", localTableColumn, foreignTableName, foreignTableColumn), matchFound);

        return this;
    }

    boolean isNewTable() {
        return isNewTable;
    }

    String getName() {
        return name;
    }
}
