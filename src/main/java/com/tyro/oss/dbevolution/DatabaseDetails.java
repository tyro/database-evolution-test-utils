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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class DatabaseDetails {

    private final String host;
    private final String port;
    private final String schemaName;
    private final String migrationUser;
    private final String migrationPassword;
    private final String adminUser;
    private final String adminPassword;
    private final String url;

    public DatabaseDetails(String host, String port, String schemaName, String migrationUser, String migrationPassword,  String adminUser, String adminPassword, String url) {
        this.host = host;
        this.port = port;
        this.schemaName = schemaName;
        this.migrationUser = migrationUser;
        this.migrationPassword = migrationPassword;
        this.adminUser = adminUser;
        this.adminPassword = adminPassword;
        this.url = url;
    }

    public static DatabaseDetails withDatabaseDetails(String migrationUser, String migrationPassword, String url) {
        return withDatabaseDetails(migrationUser, migrationPassword, null, null, url);
    }

    public static DatabaseDetails withDatabaseDetails(String migrationUser, String migrationPassword, String adminUser, String adminPassword, String url) {
        DatabaseUrl databaseUrl = new DatabaseUrl(url);
        return new DatabaseDetails(databaseUrl.getHost(), databaseUrl.getPort(), databaseUrl.getSchemaName(), migrationUser, migrationPassword, adminUser, adminPassword, url);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getMigrationUser() {
        return migrationUser;
    }

    public String getMigrationPassword() {
        return migrationPassword;
    }

    public String getAdminUser() {
        return isNotBlank(adminUser) ? adminUser : migrationUser;
    }

    public String getAdminPassword() {
        return isNotBlank(adminUser) ? adminPassword : migrationPassword;
    }

    public String getUrl() {
        return url;
    }
}

