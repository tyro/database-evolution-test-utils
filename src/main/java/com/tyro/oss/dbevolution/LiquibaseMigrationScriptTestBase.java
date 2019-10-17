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

import com.tyro.oss.dbevolution.database.DatabaseHelper;
import com.tyro.oss.dbevolution.database.DatabaseHelperFactory;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.ddlutils.model.Database;
import org.junit.jupiter.api.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.ddlutils.PlatformFactory.createNewPlatformInstance;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class LiquibaseMigrationScriptTestBase {

    protected String migrationScriptFilename;
    protected Resource schemaSnapshot;
    protected DatabaseHelper databaseHelper;

    @BeforeAll
    protected void setUpConverterForBitColumns() {
        ConvertUtilsBean convertUtils = BeanUtilsBean.getInstance().getConvertUtils();
        Converter originalBooleanConverter = convertUtils.lookup(Boolean.class);
        convertUtils.register((type, value) -> {
            if (Boolean.class.equals(type)) {
                if ("b'1'".equals(value)) {
                    return Boolean.TRUE;
                } else if ("b'0'".equals(value)) {
                    return Boolean.FALSE;
                }
            }
            return originalBooleanConverter.convert(type, value);
        }, Boolean.class);
    }

    @BeforeAll
    protected void setUpDatabase() {
        SchemaDetails schemaDetails = this.getClass().getAnnotation(SchemaDetails.class);
        MigrationScript migrationScript = this.getClass().getAnnotation(MigrationScript.class);

        try {
            schemaSnapshot = new ClassPathResource(schemaDetails.snapshotScript());
            migrationScriptFilename = migrationScript.filename();
        } catch (NullPointerException e) {
            throw new NullPointerException("Please specify a @SchemaDetails and @MigrationScript annotation in your test.");
        }

        try {
            databaseHelper = DatabaseHelperFactory.newInstance(schemaDetails, schemaSnapshot);
            databaseHelper.dropAndRecreateDatabaseFromSnapshot(schemaSnapshot);
        } catch (Exception e) {
            fail("Failed to install starting schema", e);
        }
    }

    @TestFactory
    @Order(1)
    protected Stream<DynamicTest> liquibaseMigrations() {
        return testDefinitions()
                .stream()
                .map(definition -> dynamicTest(definition.getMigrationName(), () -> testDefinitionMigration(definition)));
    }

    @Test
    @Order(2)
    protected void allScriptsShouldBeTestedAndHavePreconditionsAndAllTestedFilesIncluded() throws Exception {
        MigrationScriptsVerifier migrationScriptsVerifier = new MigrationScriptsVerifier(databaseHelper, databaseHelper.getDataSource().getConnection(), this.schemaSnapshot, migrationScriptFilename, testDefinitions());
        migrationScriptsVerifier.allScriptsShouldBeTestedAndHavePreconditionsAndAllTestedFilesIncluded();
    }

    protected abstract Collection<LiquibaseMigrationTestDefinition> testDefinitions();

    private void testDefinitionMigration(LiquibaseMigrationTestDefinition definition) throws SQLException, LiquibaseException {
        try (Connection connection = databaseHelper.getConnection()) {
            definition.assertPreMigrationSchema(getDatabase(), connection);
            if (definition.disableReferentialIntegrityForInsertingPreMigrationData()) {
                setReferentialIntegrity(false);
            }
            try {
                definition.insertPreMigrationData(connection);
                connection.commit();
            } finally {
                setReferentialIntegrity(true);
            }
            definition.assertPreMigrationData(connection);
            executeScript(definition);
            definition.assertPostMigrationSchema(getDatabase(), connection);
            definition.assertPostMigrationData(connection);
            connection.commit();
        }
    }

    private void setReferentialIntegrity(boolean on) throws SQLException {
        try (Statement statement = databaseHelper.getConnection().createStatement()) {
            statement.execute(format("SET FOREIGN_KEY_CHECKS = %s", on ? "1" : "0"));
        }
    }

    private Database getDatabase() throws SQLException {
        return createNewPlatformInstance(databaseHelper.getDataSource())
                .readModelFromDatabase(
                        databaseHelper.getConnection(),
                        databaseHelper.getDatabaseDetails().getSchemaName(),
                        databaseHelper.getDatabaseDetails().getSchemaName(),
                        null,
                        null);
    }

    private void executeScript(LiquibaseMigrationTestDefinition definition) throws SQLException, LiquibaseException {
        String migrationScript = definition.getMigrationScriptFilename();
        Liquibase migrator = new Liquibase(migrationScript, new ClassLoaderResourceAccessor(), new JdbcConnection(databaseHelper.getConnection()));
        migrator.update("production");
    }
}
