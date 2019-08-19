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
import com.tyro.oss.dbevolution.LiquiBaseMigrationScriptTestBase;
import com.tyro.oss.dbevolution.SchemaDetails;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.IOException;

import static com.tyro.oss.dbevolution.DatabaseDetails.withDatabaseDetails;

public class DatabaseHelperFactory {

    public static DatabaseHelper newInstance(DatabaseDetails databaseDetails, Resource defaultSchemaResource) {
        return new MySqlDatabaseHelper(databaseDetails, defaultSchemaResource);
    }

    public static DatabaseHelper newInstance(SchemaDetails schemaDetails) throws IOException {
        File schemaTempFile = File.createTempFile(LiquiBaseMigrationScriptTestBase.class.getSimpleName(), ".tmp.sql");
        DatabaseDetails databaseDetails = withDatabaseDetails(
                schemaDetails.migrationUser(),
                schemaDetails.migrationPassword(),
                schemaDetails.adminUser(),
                schemaDetails.adminPassword(),
                schemaDetails.url());
        return new MySqlDatabaseHelper(databaseDetails, schemaTempFile);
    }
}
