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
package com.tyro.oss.dbevolution;

import org.apache.ddlutils.model.Database;

import java.sql.Connection;
import java.time.LocalDateTime;

import static com.tyro.oss.dbevolution.assertions.Assertions.assertThatSchema;

public class CreateExampleTable extends LiquiBaseMigrationTestDefinition {

    @Override
    protected void assertPreMigrationSchema(Database schema, Connection connection) {
        assertThatSchema(schema, connection)
                .doesNotHaveTable("ExampleTable");
    }

    @Override
    protected void assertPostMigrationSchema(Database schema, Connection connection) {
        assertThatSchema(schema, connection)
                .hasTable("ExampleTable")
                .enterNewTableAssertionMode()
                .hasColumn("id")
                    .supportsIdType()
                    .isPrimaryKey()
                    .isNotNullable()
                    .isAutoIncrementing()
                .hasColumn("column1")
                    .supportsType(String.class)
                    .isNullable()
                .hasColumn("column2")
                    .supportsType(Long.class)
                    .isNullable()
                .hasColumn("column3")
                    .supportsType(Boolean.class)
                    .isNullable()
                .hasColumn("column4")
                    .supportsType(LocalDateTime.class)
                    .isNotNullable()
                .andTable()
                .hasIndexOn("column1", "column2")
                .hasNoIndexOn("column3")
                .hasUniqueIndexOn("column4");
    }
}
