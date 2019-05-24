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

import org.apache.commons.lang3.StringUtils;
import org.apache.ddlutils.model.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class LiquiBaseMigrationTestDefinition {

    private final String migrationName;
    private final String migrationScriptLocation;
    private final boolean allowAnyPreconditionOnFailHandling;

    public LiquiBaseMigrationTestDefinition() {
        this.allowAnyPreconditionOnFailHandling = false;
        this.migrationName = getClass().getSimpleName();
        this.migrationScriptLocation = deriveMigrationScriptLocationFromTestPackage();
    }

    public LiquiBaseMigrationTestDefinition(boolean allowAnyPreconditionOnFailHandling) {
        this.allowAnyPreconditionOnFailHandling = allowAnyPreconditionOnFailHandling;
        this.migrationName = getClass().getSimpleName();
        this.migrationScriptLocation = deriveMigrationScriptLocationFromTestPackage();
    }

    public LiquiBaseMigrationTestDefinition(String migrationScriptLocation) {
        this.allowAnyPreconditionOnFailHandling = false;
        this.migrationName = getClass().getSimpleName();
        this.migrationScriptLocation = migrationScriptLocation;
    }

    protected static List<List<String>> executeQuery(String query, Connection connection) throws SQLException {
        List<List<String>> results = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                List<String> result = new ArrayList<>();
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    String bin = resultSet.getString(i);
                    result.add(bin);
                }
                results.add(result);
            }
        }
        return results;
    }

    private String deriveMigrationScriptLocationFromTestPackage() {
        String migrationTestPackage = StringUtils.substringAfterLast(getClass().getPackage().getName(), ".");
        return migrationTestPackage.matches("\\Qrelease_\\E\\d\\d[_]\\d\\d[_]\\d\\d") ?
                migrationTestPackage.substring("release_".length()).replace('_', '.') :
                migrationTestPackage;
    }

    public final String getMigrationScriptFilename() {
        return getMigrationScriptLocation() + "/" + getMigrationName() + ".xml";
    }

    public final String getMigrationName() {
        return migrationName;
    }

    final String getMigrationScriptLocation() {
        return migrationScriptLocation;
    }

    protected void assertPreMigrationSchema(Database schema, Connection connection) {
        throw new UnsupportedOperationException("You must override either assertPreMigrationSchema(Database) or assertPreMigrationSchema(Database, Connection)");
    }

    protected void assertPostMigrationSchema(Database schema, Connection connection) {
        throw new UnsupportedOperationException("You must override either assertPostMigrationSchema(Database) or assertPostMigrationSchema(Database, Connection)");
    }

    protected void insertPreMigrationData(Connection connection) throws SQLException {
    }

    protected void assertPreMigrationData(Connection connection) throws SQLException {
    }

    protected void assertPostMigrationData(Connection connection) throws SQLException {
    }

    public boolean disableReferentialIntegrityForInsertingPreMigrationData() {
        return false;
    }

    public boolean isAllowAnyPreconditionOnFailHandling() {
        return allowAnyPreconditionOnFailHandling;
    }
}

