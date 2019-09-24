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
package com.tyro.oss.dbevolution.database;

import com.tyro.oss.dbevolution.DatabaseDetails;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.String.format;

public class MySqlDatabaseHelper implements DatabaseHelper {

    private static final Log LOG = LogFactory.getLog(MySqlDatabaseHelper.class);

    private static BasicDataSource migrationDataSource;
    private static BasicDataSource adminDataSource;

    private final DatabaseDetails databaseDetails;
    private final Resource defaultSchemaResource;
    private final CommandLineHelper commandLineHelper;
    private Connection connection;

    public MySqlDatabaseHelper(DatabaseDetails databaseDetails, Resource defaultSchemaResource) {
        this.databaseDetails = databaseDetails;
        this.defaultSchemaResource = defaultSchemaResource;
        this.commandLineHelper = new CommandLineHelper();
    }

    @Override
    public DatabaseDetails getDatabaseDetails() {
        return databaseDetails;
    }

    @Override
    public String getUrl() {
        return databaseDetails.getUrl();
    }

    @Override
    public DataSource getDataSource() {
        if (migrationDataSource == null) {
            migrationDataSource = new BasicDataSource();
            migrationDataSource.setUrl(getUrl());
            migrationDataSource.setUsername(databaseDetails.getMigrationUser());
            migrationDataSource.setPassword(databaseDetails.getMigrationPassword());
            migrationDataSource.setDefaultAutoCommit(false);
            migrationDataSource.setMaxIdle(10);
        }
        return migrationDataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = getDataSource().getConnection();
        }
        return connection;
    }

    @Override
    public void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            connection = null;
        }
    }

    @Override
    public void dropAndRecreateDatabase() throws CommandExecutionException {
        executeStatement(
                format("drop database if exists %%1$s; create database %1$s;",
                        databaseDetails.getSchemaName()));
    }

    @Override
    public void dropAndRecreateDatabaseFromSnapshot() throws CommandExecutionException, SQLException {
        closeConnection();
        dropAndRecreateDatabaseFromSnapshot(defaultSchemaResource);
    }

    @Override
    public void dropAndRecreateDatabaseFromSnapshot(Resource schemaFile) throws CommandExecutionException {
        File fileToSource = writeSnapshotThatMightBeInAJarFileToAPlainOldFileOnDisk(schemaFile);
        dropAndRecreateDatabaseFromSnapshotThatIsAlreadyOnDisk(fileToSource);
    }

    @Override
    public void dropAndRecreateDatabaseFromSnapshotThatIsAlreadyOnDisk(File absoluteFileName) throws CommandExecutionException {
        executeStatement("drop database if exists " + databaseDetails.getSchemaName());
        executeStatement("create database " + databaseDetails.getSchemaName());
        executeStatement("use " + databaseDetails.getSchemaName());
        executeScript(absoluteFileName);
    }

    @Override
    public void createSnapshot(File targetFile, boolean includeData) throws CommandExecutionException, IOException {
        createSchemaSnapshot(targetFile, includeData);
        createTablePrivilegesSnapshot(targetFile);
    }

    private File writeSnapshotThatMightBeInAJarFileToAPlainOldFileOnDisk(Resource schemaFile) throws CommandExecutionException {
        File fileToSource;
        try {
            fileToSource = schemaFile.getFile();
        } catch (IOException e) {
            LOG.debug("Snapshot file does not exist locally. Copying from jar so mysql can source it.");
            try {
                fileToSource = File.createTempFile(defaultSchemaResource.getFilename(), ".sql");
                fileToSource.deleteOnExit();
                IOUtils.copy(schemaFile.getInputStream(), new FileOutputStream(fileToSource));
            } catch (IOException e2) {
                LOG.error("Error creating temp copy of database snapshot", e2);
                throw new CommandExecutionException(e2);
            }
        }
        return fileToSource;
    }

    private void createSchemaSnapshot(File targetFile, boolean includeData) throws CommandExecutionException {
        commandLineHelper.executeCommand(new String[]{
                "mysqldump",
                "--user=" + databaseDetails.getAdminUser(),
                "--password=" + databaseDetails.getAdminPassword(),
                "--host=" + databaseDetails.getHost(),
                "--port=" + getPort(),
                "--no-data=" + (includeData ? "false" : "true"),
                databaseDetails.getSchemaName()}, targetFile, false);
    }

    private void createTablePrivilegesSnapshot(File targetFile) throws CommandExecutionException, IOException {
        appendToSnapshot(targetFile, "-- Switching to mysql database to migrate tables_priv");
        appendToSnapshot(targetFile, "USE mysql;");
        commandLineHelper.executeCommand(new String[]{
                "mysqldump",
                "--user=" + databaseDetails.getAdminUser(),
                "--password=" + databaseDetails.getAdminPassword(),
                "--host=" + databaseDetails.getHost(),
                "--port=" + getPort(),
                "--no-create-info",
                "--replace", "mysql",
                "--tables", "tables_priv",
                "--where=Db='" + databaseDetails.getSchemaName() + "'"}, targetFile, true);
        appendToSnapshot(targetFile, "FLUSH PRIVILEGES;");
    }

    private void executeStatement(String statement) throws CommandExecutionException {
        DataSource dataSource = getAdminDataSource();
        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().execute(statement);
        } catch (SQLException e) {
            throw new CommandExecutionException(e);
        }
    }

    private void executeScript(File scriptFile) throws CommandExecutionException {
        DataSource dataSource = getAdminDataSource();
        try (Connection connection = dataSource.getConnection()) {
            Reader reader = new BufferedReader(new FileReader(scriptFile.getAbsoluteFile()));
            ScriptRunner scriptRunner = new ScriptRunner(connection);
            scriptRunner.setLogWriter(null);
            scriptRunner.runScript(reader);
        } catch (Exception e) {
            throw new CommandExecutionException(e);
        }
    }

    private int getPort() {
        return databaseDetails.getPort() == -1 ? 3306 : databaseDetails.getPort();
    }

    private void appendToSnapshot(File targetFile, String str) throws IOException {
        try (Writer writer = new FileWriter(targetFile, true)) {
            writer.write(str);
            writer.write("\n");
        }
    }

    private DataSource getAdminDataSource() {
        if (adminDataSource == null) {
            adminDataSource = new BasicDataSource();
            adminDataSource.setUrl(getUrl());
            adminDataSource.setUsername(databaseDetails.getAdminUser());
            adminDataSource.setPassword(databaseDetails.getAdminPassword());
        }
        return adminDataSource;
    }
}
