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
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

public class SnapshotAndLiquibaseMigrationStrategy implements DatabaseCreationStrategy {

    private final DatabaseSnapshot snapshot;
    private final File migratedSnapshotFile;
    private boolean temporarySnapshotPopulated;

    public SnapshotAndLiquibaseMigrationStrategy(DatabaseSnapshot snapshot) {
        this.snapshot = snapshot;
        try {
            this.migratedSnapshotFile = File.createTempFile("migrated.snapshot." + snapshot.getSnapshotResource().getFilename(), ".xml");
            this.migratedSnapshotFile.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp file to hold migrated snapshot", e);
        }
    }

    @Override
    public void createDatabase(DatabaseDetails databaseDetails, String migrationScriptsFilename) throws Exception {
        DatabaseHelper databaseHelper = DatabaseHelperFactory.newInstance(databaseDetails, snapshot.getSnapshotResource());

        if (temporarySnapshotPopulated) {
            databaseHelper.dropAndRecreateDatabaseFromSnapshotThatIsAlreadyOnDisk(migratedSnapshotFile);
        } else {
            loadUnmigratedSnaphostAndMigrateUsingLiquibase(databaseHelper, migrationScriptsFilename);
            saveTemporarySnapshotToSaveRemigrating(databaseHelper);
        }
    }

    private void saveTemporarySnapshotToSaveRemigrating(DatabaseHelper databaseHelper) throws CommandExecutionException, IOException {
        databaseHelper.createSnapshots(migratedSnapshotFile, true);
        temporarySnapshotPopulated = true;
    }

    private void loadUnmigratedSnaphostAndMigrateUsingLiquibase(DatabaseHelper databaseHelper, String migrationScriptsFilename) throws Exception {
        databaseHelper.dropAndRecreateDatabaseFromSnapshot();
        try (Connection connection = databaseHelper.getDataSource().getConnection()) {
            Liquibase liquibase = new Liquibase(migrationScriptsFilename, new ClassLoaderResourceAccessor(), new JdbcConnection(connection));
            liquibase.update("test");
        }
    }

    public File getMigratedSnapshotFile() {
        return migratedSnapshotFile;
    }
}

