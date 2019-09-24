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
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseHelper {

    DatabaseDetails getDatabaseDetails();

    String getUrl();

    DataSource getDataSource();

    Connection getConnection() throws SQLException;

    void closeConnection() throws SQLException;

    void dropAndRecreateDatabase() throws CommandExecutionException;

    void dropAndRecreateDatabaseFromSnapshot() throws CommandExecutionException, SQLException;

    void dropAndRecreateDatabaseFromSnapshot(Resource schemaFile) throws CommandExecutionException;

    void dropAndRecreateDatabaseFromSnapshotThatIsAlreadyOnDisk(File absoluteFileName) throws CommandExecutionException;

    void createSnapshot(File targetFile, boolean includeData) throws CommandExecutionException, IOException;
}
