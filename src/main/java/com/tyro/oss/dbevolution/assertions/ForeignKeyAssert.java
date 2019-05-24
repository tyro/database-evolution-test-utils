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
import org.apache.ddlutils.model.ForeignKey;

import static org.junit.Assert.*;

public class ForeignKeyAssert {

    private final ForeignKey foreignKey;
    private final String keyAsString;
    private final Database schema;
    private final ColumnAssert sourceColumn;

    public ForeignKeyAssert(Database schema, ColumnAssert sourceColumn, String targetTable, String targetColumn) {
        this.schema = schema;
        this.sourceColumn = sourceColumn;
        this.keyAsString = sourceColumn.andTable().getName() + "." + sourceColumn.getName() + " -> " + targetTable + "." + targetColumn;
        this.foreignKey = getForeignKey(sourceColumn, targetTable, targetColumn);
    }

    public ColumnAssert andColumn() {
        return sourceColumn;
    }

    private ForeignKey getForeignKey(ColumnAssert sourceColumn, String targetTable, String targetColumn) {
        for (ForeignKey foreignKey : schema.findTable(sourceColumn.andTable().getName()).getForeignKeys()) {
            if (foreignKey.getFirstReference().getLocalColumnName().equals(sourceColumn.getName())
                    && foreignKey.getForeignTableName().equals(targetTable)
                    && foreignKey.getFirstReference().getForeignColumnName().equals(targetColumn)
                    && foreignKey.getReferenceCount() == 1) {
                return foreignKey;
            }
        }
        return null;
    }

    public ForeignKeyAssert isPresent() {
        assertNotNull("Foreign Key not found for " + keyAsString, foreignKey);
        return this;
    }

    public ForeignKeyAssert isNotPresent() {
        assertNull("Foreign Key exists for " + keyAsString, foreignKey);
        return this;
    }

    public ForeignKeyAssert withName(String expectedKeyName) {
        isPresent();
        assertEquals("Name of Foreign Key " + keyAsString, expectedKeyName, foreignKey.getName());
        return this;
    }

    public TableAssert andTable() {
        return sourceColumn.andTable();
    }

    public ColumnAssert hasColumn(String columnName) {
        return sourceColumn.hasColumn(columnName);
    }
}
