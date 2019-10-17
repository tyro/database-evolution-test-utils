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
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.database.jvm.JdbcConnection;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.precondition.core.PreconditionContainer;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.springframework.core.io.Resource;

import java.sql.Connection;
import java.util.*;

import static java.lang.String.format;
import static liquibase.precondition.core.PreconditionContainer.ErrorOption;
import static liquibase.precondition.core.PreconditionContainer.FailOption;
import static org.junit.jupiter.api.Assertions.*;

public class MigrationScriptsVerifier {

    private final DatabaseHelper databaseHelper;
    private final Connection connection;
    private final String migrationScriptsFilename;
    private final Resource schemaFile;
    private final List<String> changeSetFilesUnderTest;
    private final Set<String> changesetPreconditionExclusions;
    private List<String> newChangeSetFilesCompleted;
    private DatabaseChangeLog changeLog;

    MigrationScriptsVerifier(DatabaseHelper databaseHelper, Connection connection, Resource schemaFile, String migrationScriptsFilename, Collection<LiquiBaseMigrationTestDefinition> testDefinitions) {
        this.databaseHelper = databaseHelper;
        this.connection = connection;
        this.schemaFile = schemaFile;
        this.migrationScriptsFilename = migrationScriptsFilename;
        this.changesetPreconditionExclusions = new HashSet<>();

        changeSetFilesUnderTest = new ArrayList<>();
        for (LiquiBaseMigrationTestDefinition testDefinition : testDefinitions) {
            changeSetFilesUnderTest.add(testDefinition.getMigrationScriptFilename());
            if (testDefinition.isAllowAnyPreconditionOnFailHandling()) {
                changesetPreconditionExclusions.add(testDefinition.getMigrationScriptFilename());
            }
        }
    }

    private void setUp() throws Exception {
        databaseHelper.dropAndRecreateDatabaseFromSnapshot(schemaFile);

        Liquibase migrator = new Liquibase(migrationScriptsFilename, new ClassLoaderResourceAccessor(), new JdbcConnection(connection));

        try {
            changeLog = ChangeLogParserFactory.getInstance().getParser(migrationScriptsFilename, migrator.getResourceAccessor()).parse(migrationScriptsFilename, migrator.getChangeLogParameters(), migrator.getResourceAccessor());
            migrator.checkLiquibaseTables(true, changeLog, new Contexts("production"), new LabelExpression("production"));

            List<RanChangeSet> changeSetsCompletedBeforeMigration = new ArrayList<>(migrator.getDatabase().getRanChangeSetList());
            migrator.update("production");
            List<RanChangeSet> changeSetsCompletedAfterMigration = migrator.getDatabase().getRanChangeSetList();

            newChangeSetFilesCompleted = new ArrayList<>();
            for (RanChangeSet changeSet : changeSetsCompletedAfterMigration) {
                newChangeSetFilesCompleted.add(changeSet.getChangeLog());
            }
            for (RanChangeSet changeSet : changeSetsCompletedBeforeMigration) {
                newChangeSetFilesCompleted.remove(changeSet.getChangeLog());
            }
        } finally {
            connection.close();
        }
    }

    public void allScriptsShouldBeTestedAndHavePreconditionsAndAllTestedFilesIncluded() throws Exception {
        setUp();
        checkAllChangeLogsNamedInTestsHaveBeenExecuted();
        checkAllChangeLogsNamedInScriptListFileHaveBeenTested();
        checkAllChangeLogsHaveAppropriatePreconditions();
    }

    private void checkAllChangeLogsHaveAppropriatePreconditions() {
        List<ChangeSet> changeSets = changeLog.getChangeSets();
        for (ChangeSet changeSet : changeSets) {
            String changeSetName = changeSet.getFilePath();

            PreconditionContainer preconditions = changeSet.getPreconditions();

            assertNotNull(preconditions,
                    format("%s has no preconditions. Preconditions are required.", changeSetName));

            assertEquals(ErrorOption.HALT, preconditions.getOnError(),
                    format("%s should HALT on error.", changeSetName));

            if (!changesetPreconditionExclusions.contains(changeSetName)) {
                assertEquals(FailOption.HALT, preconditions.getOnFail(),
                        format("%s should HALT on fail.", changeSetName));
            } else {
                System.out.println("Skipping precondition check for " + changeSetName);
            }
        }
    }

    private void checkAllChangeLogsNamedInTestsHaveBeenExecuted() {
        for (String changeSetInDatabaseMigrationScriptsFile : newChangeSetFilesCompleted) {
            assertTrue(changeSetFilesUnderTest.contains(changeSetInDatabaseMigrationScriptsFile),
                    "There is no test listed for the " + changeSetInDatabaseMigrationScriptsFile + " migration script.");
        }
    }

    private void checkAllChangeLogsNamedInScriptListFileHaveBeenTested() {
        for (String changeSetUnderTest : changeSetFilesUnderTest) {
            boolean isExtraReleaseScript = changeSetUnderTest.startsWith("extra-release");
            if (!isExtraReleaseScript) {
                assertTrue(newChangeSetFilesCompleted.contains(changeSetUnderTest),
                        format("The %s migration script does not appear in %s or was already run before the current snapshot.", changeSetUnderTest, migrationScriptsFilename));
            } else {
                assertFalse(newChangeSetFilesCompleted.contains(changeSetUnderTest),
                        format("The %s migration script SHOULD NOT appear in %s because it is an extra-release script and should not be run automatically.", changeSetUnderTest, migrationScriptsFilename));
            }
        }
    }
}
