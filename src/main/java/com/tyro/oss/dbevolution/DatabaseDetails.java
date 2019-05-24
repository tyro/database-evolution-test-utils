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

public class DatabaseDetails {

    private final String host;
    private final String port;
    private final String schemaName;
    private final String username;
    private final String password;
    private final String url;

    public DatabaseDetails(String host, String port, String schemaName, String username, String password, String url) {
        this.host = host;
        this.port = port;
        this.schemaName = schemaName;
        this.username = username;
        this.password = password;
        this.url = url;
    }

    public static DatabaseDetails withDatabaseDetails(String username, String password, String url) {
        DatabaseUrl databaseUrl = new DatabaseUrl(url);
        return new DatabaseDetails(databaseUrl.getHost(), databaseUrl.getPort(), databaseUrl.getSchemaName(), username, password, url);
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

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        return url;
    }
}

