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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static liquibase.precondition.core.PreconditionContainer.ErrorOption;
import static liquibase.precondition.core.PreconditionContainer.FailOption;
import static org.junit.Assert.*;

public class MigrationScriptsVerifier {

    private final DatabaseHelper databaseHelper;
    private final Connection connection;
    private final String migrationScriptsFilename;
    private final Resource schemaFile;
    private List<String> newChangeSetFilesCompleted;
    private List<String> changeSetFilesUnderTest;
    private DatabaseChangeLog changeLog;
    private Set<String> changesetPreconditionExclusions;

    MigrationScriptsVerifier(DatabaseHelper databaseHelper, Connection connection, Resource schemaFile, String migrationScriptsFilename, List<LiquiBaseMigrationTestDefinition> testDefinitions) {
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
            String comments = changeSet.getComments();
            String changeSetName = changeSet.getFilePath();
            assertNotNull(changeSetName + " requires a comment specifying the release version.", comments);

            PreconditionContainer preconditions = changeSet.getPreconditions();

            assertNotNull(changeSetName + " has no preconditions. Preconditions are required.", preconditions);

            assertEquals(changeSetName + " should HALT on error.", ErrorOption.HALT, preconditions.getOnError());

            if (!changesetPreconditionExclusions.contains(changeSetName)) {
                assertEquals(changeSetName + " should HALT on fail.", FailOption.HALT, preconditions.getOnFail());
            } else {
                System.out.println("Skipping precondition check for " + changeSetName);
            }
        }
    }

    private void checkAllChangeLogsNamedInTestsHaveBeenExecuted() {
        for (String changeSetInDatabaseMigrationScriptsFile : newChangeSetFilesCompleted) {
            assertTrue("There is no test listed for the " + changeSetInDatabaseMigrationScriptsFile + " migration script.",
                    changeSetFilesUnderTest.contains(changeSetInDatabaseMigrationScriptsFile));
        }
    }

    private void checkAllChangeLogsNamedInScriptListFileHaveBeenTested() {
        for (String changeSetUnderTest : changeSetFilesUnderTest) {
            boolean isExtraReleaseScript = changeSetUnderTest.startsWith("extra-release");
            if (!isExtraReleaseScript) {
                assertTrue("The " + changeSetUnderTest + " migration script does not appear in " + migrationScriptsFilename + " or was already run before the current snapshot.",
                        newChangeSetFilesCompleted.contains(changeSetUnderTest));
            } else {
                assertFalse("The " + changeSetUnderTest + " migration script SHOULD NOT appear in " + migrationScriptsFilename + " because it is an extra-release script and should not be run automatically.",
                        newChangeSetFilesCompleted.contains(changeSetUnderTest));
            }
        }
    }
}
