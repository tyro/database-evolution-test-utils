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

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static java.util.Arrays.asList;

@Testcontainers
@SchemaDetails(
        migrationUser = "test",
        migrationPassword = "test",
        url = "jdbc:tc:mysql://localhost/test?serverTimezone=UTC",
        snapshotScript = "schema.sql")
@MigrationScript(filename = "migration-scripts.xml")
public class LiquibaseScriptsTest extends LiquibaseMigrationScriptTestBase {

    @Container
    private final MySQLContainer mysql = new MySQLContainer();

    @Override
    protected List<LiquibaseMigrationTestDefinition> testDefinitions() {
        return asList(new CreateExampleTable());
    }
}

