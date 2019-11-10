# database-evolution-test-utils

[![Download](https://maven-badges.herokuapp.com/maven-central/com.tyro.oss/database-evolution-test-utils/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.tyro.oss/database-evolution-test-utils)
[![Build Status](https://travis-ci.org/tyro/database-evolution-test-utils.svg?branch=master)](https://travis-ci.org/tyro/database-evolution-test-utils)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

A simple library for testing liquibase database migrations.

## Getting Started

database-evolution-test-utils is available on Maven Central.
```xml
<dependency>
    <groupId>com.tyro.oss</groupId>
    <artifactId>database-evolution-test-utils</artifactId>
    <version>1.0</version>
    <scope>test</scope>
</dependency>
```

### Example

This example shows how we can use <b>database-evolution-test-utils</b> to test liquibase
scripts for database migrations.

#### Folder structure overview
```text
├── src
│   ├── main
│   │   ├── java
│   │   └── resources
│   │       ├── application.properties
│   │       ├── migration-scripts.xml
│   │       └── dbevolution
│   │           └── CreateExampleTable.xml
│   └── test
│       ├── java
│       │   └── com
│       │       └── tyro
│       │           └── oss
│       │               └── dbevolution
│       │                   └── CreateExampleTable.java
│       │                   ├── LiquibaseScriptsTest.java
│       └── resources
│           └── schema.sql

```

The following in an example Changeset to create a table called <b>ExampleTable</b> with multiple columns.

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<databaseChangeLog xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
   http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.6.xsd">

    <changeSet id='20191001' author='sorourke'>
        <preConditions>
            <not>
                <tableExists tableName="ExampleTable"/>
            </not>
        </preConditions>

        <createTable tableName="ExampleTable">
            <column name="id" type="bigint" autoIncrement="true">
                <constraints primaryKey="true" nullable="false"/>
            </column>
            <column name="column1" type="varchar(255)"/>
            <column name="column2" type="bigint"/>
            <column name="column3" type="bit(1)"/>
            <column name="column4" type="datetime">
                <constraints nullable="false"/>
            </column>
        </createTable>

        <createIndex tableName="ExampleTable" indexName="idx_ExampleTable_column1_column2">
            <column name="column1"/>
            <column name="column2"/>
        </createIndex>

        <createIndex tableName="ExampleTable" indexName="idx_ExampleTable_column4" unique="true">
            <column name="column4"/>
        </createIndex>

    </changeSet>
</databaseChangeLog>
```

Here's the associate liquibase Changelog file with the create table Changeset:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<databaseChangeLog xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
   http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.6.xsd">

    <include file="dbevolution/CreateExampleTable.xml" />

</databaseChangeLog>

```

### Testing

Here's how we test whether the Changesets are created correctly.

Define <b>LiquibaseMigrationTestDefinition</b> for each Changeset.

<b>NOTE</b>: It needs to be the <b>same name</b> as the one defined in 
<b>migration-scripts.xml</b>, and also should be under the <b>same directory</b>, 
<i>i.e. dbevolution/CreateExampleTable.java</i>

```java
public class CreateExampleTable extends LiquibaseMigrationTestDefinition {

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
                    .isPrimaryKeyIdColumn()
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

```

Creates test class that extends the <b>LiquibaseMigrationScriptTestBase</b> class, and include all the 
<b>LiquibaseMigrationTestDefinitions</b> in it.

```java
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
```

That's it!!! Happy migrating!

## Copyright and Licensing

Copyright (C) 2019 Tyro Payments Pty Ltd

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.
