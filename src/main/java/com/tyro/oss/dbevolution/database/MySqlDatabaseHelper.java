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
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

import static java.lang.String.format;

public class MySqlDatabaseHelper implements DatabaseHelper {

    private static final Log LOG = LogFactory.getLog(MySqlDatabaseHelper.class);

    private static BasicDataSource basicDataSource;

    private final DatabaseDetails databaseDetails;
    private final Resource defaultSchemaResource;
    private final CommandLineHelper commandLineHelper;
    private Connection connection;

    public MySqlDatabaseHelper(DatabaseDetails databaseDetails, File defaultSchemaFile) {
        this(databaseDetails, new FileSystemResource(defaultSchemaFile));
    }

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
        if (basicDataSource == null) {
            basicDataSource = new BasicDataSource();
            basicDataSource.setUrl(getUrl());
            basicDataSource.setUsername(databaseDetails.getMigrationUser());
            basicDataSource.setPassword(databaseDetails.getMigrationPassword());
            basicDataSource.setDefaultAutoCommit(false);
            basicDataSource.setMaxIdle(10);
        }
        return basicDataSource;
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
                format("drop database if exists %s; create database %s",
                        databaseDetails.getSchemaName(),
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
        executeStatement(
                format("drop database if exists %s; create database %s; use %s; \\. %s",
                        databaseDetails.getSchemaName(),
                        databaseDetails.getSchemaName(),
                        databaseDetails.getSchemaName(),
                        absoluteFileName.getAbsoluteFile()));
    }

    @Override
    public void createSnapshot(boolean includeData) throws CommandExecutionException {
        try {
            File snapshotFile = defaultSchemaResource.getFile();
            snapshotFile.deleteOnExit();
            createSnapshot(snapshotFile, includeData);
        } catch (IOException e) {
            throw new CommandExecutionException(e);
        }
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
        commandLineHelper.executeCommand(new String[]{
                "mysql",
                "--user=" + databaseDetails.getAdminUser(),
                "--password=" + databaseDetails.getAdminPassword(),
                "--host=" + databaseDetails.getHost(),
                "--port=" + getPort(),
                "--execute=" + statement});
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
}
