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

import com.tyro.oss.dbevolution.database.CommandExecutionException;
import com.tyro.oss.dbevolution.database.DatabaseHelper;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.ddlutils.model.Database;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.Resource;

import java.sql.*;

import static org.apache.ddlutils.PlatformFactory.createNewPlatformInstance;
import static org.junit.Assert.*;

public abstract class LiquiBaseMigrationScriptTestBase {

    public static DatabaseHelper databaseHelper;
    public static Resource schemaSnapshot;
    public static MigrationScriptsVerifier migrationScriptsVerifier;

    LiquiBaseMigrationTestDefinition definition;

    @BeforeClass
    public static void runOnce() {
        try {
            databaseHelper.dropAndRecreateDatabaseFromSnapshot(schemaSnapshot);
        } catch (Exception e) {
            System.err.println("Failed to install starting schema: " + e);
            e.printStackTrace();
            fail("Failed to install starting schema: " + e);
        }
    }

    @BeforeClass
    public static void setUpConverterForBitColumns() {
        ConvertUtilsBean convertUtils = BeanUtilsBean.getInstance().getConvertUtils();
        final Converter originalBooleanConverter = convertUtils.lookup(Boolean.class);
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

    @Test
    public void testDefinitionMigration() throws SQLException, LiquibaseException, CommandExecutionException {
        databaseHelper.createSnapshot(true);

        try {
            definition.assertPreMigrationSchema(getDatabase(), getConnection());
            if (definition.disableReferentialIntegrityForInsertingPreMigrationData()) {
                setReferentialIntegrity(false);
            }
            try {
                definition.insertPreMigrationData(getConnection());
                getConnection().commit();
            } finally {
                setReferentialIntegrity(true);
            }
            definition.assertPreMigrationData(getConnection());
            executeScript();
            definition.assertPostMigrationSchema(getDatabase(), getConnection());
            definition.assertPostMigrationData(getConnection());

            runTheMigrationScriptWithoutPreMigrationData();
        } finally {
            getConnection().close();
        }
    }

    public void allScriptsShouldBeTestedAndHavePreconditionsAndAllTestedFilesIncluded() throws Exception {
        migrationScriptsVerifier.allScriptsShouldBeTestedAndHavePreconditionsAndAllTestedFilesIncluded();
    }

    private void runTheMigrationScriptWithoutPreMigrationData() throws CommandExecutionException, SQLException, LiquibaseException {
        databaseHelper.dropAndRecreateDatabaseFromSnapshot();
        Liquibase migrator = new Liquibase(definition.getMigrationScriptFilename(), new ClassLoaderResourceAccessor(), new JdbcConnection(getConnection()));
        migrator.update("production");
    }

    private void setReferentialIntegrity(boolean on) throws SQLException {
        String statementString = "SET FOREIGN_KEY_CHECKS = " + (on ? "1" : "0");
        Statement statement = databaseHelper.getConnection().createStatement();
        statement.execute(statementString);
        statement.close();
    }

    private Connection getConnection() throws SQLException {
        return databaseHelper.getConnection();
    }

    private Database getDatabase() throws SQLException {
        return createNewPlatformInstance(databaseHelper.getDataSource()).readModelFromDatabase(
                getConnection(),
                databaseHelper.getDatabaseDetails().getSchemaName(),
                databaseHelper.getDatabaseDetails().getSchemaName(),
                null,
                null);
    }

    private void executeScript() throws SQLException, LiquibaseException {
        String migrationScript = definition.getMigrationScriptFilename();
        Liquibase migrator = new Liquibase(migrationScript, new ClassLoaderResourceAccessor(), new JdbcConnection(getConnection()));
        migrator.update("production");

        PreparedStatement ps = databaseHelper.getConnection().prepareStatement("select COMMENTS from DATABASECHANGELOG where FILENAME = ?");
        ps.setString(1, migrationScript);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        String comments = rs.getString("COMMENTS");
        assertNotNull("The " + migrationScript + " change log requires a comment tag", comments);
        assertEquals("Comments tag should match release (migration script location) in " + migrationScript, definition.getMigrationScriptLocation(), comments);
    }
}

