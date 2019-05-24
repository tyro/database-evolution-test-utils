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
import com.tyro.oss.dbevolution.database.DatabaseHelperFactory;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.TestClass;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class LiquiBaseMigrationScriptsTestRunner extends BlockJUnit4ClassRunner {

    private final List<FrameworkMethod> children = new ArrayList<>();
    private final DatabaseHelper databaseHelper;
    private final String migrationScriptFilename;
    private final Resource schemaSnapshot;
    private final MigrationScriptsVerifier migrationScriptsVerifier;

    public LiquiBaseMigrationScriptsTestRunner(Class<? extends LiquiBaseMigrationScriptTestBase> clazz) throws Exception {
        super(clazz);

        SchemaDetails schemaDetails = clazz.getAnnotation(SchemaDetails.class);
        MigrationScript migrationScript = clazz.getAnnotation(MigrationScript.class);

        try {
            this.schemaSnapshot = new ClassPathResource(schemaDetails.snapshotScript());
            this.migrationScriptFilename = migrationScript.filename();
        } catch (NullPointerException npe) {
            throw new NullPointerException("Please specify a @SchemaDetails and @MigrationScript annotation in your test.");
        }

        databaseHelper = DatabaseHelperFactory.newInstance(schemaDetails);

        DataSource dataSource = databaseHelper.getDataSource();

        try {
            Method declaredMethod = clazz.getDeclaredMethod("getTestDefinitions");
            List<LiquiBaseMigrationTestDefinition> testDefinitions = (List<LiquiBaseMigrationTestDefinition>) declaredMethod.invoke(null, null);

            for (LiquiBaseMigrationTestDefinition testDefinition : testDefinitions) {
                children.add(new LiquiBaseMigrationScriptTesterFrameworkMethod(testDefinition));
            }

            migrationScriptsVerifier = new MigrationScriptsVerifier(databaseHelper, dataSource.getConnection(), this.schemaSnapshot, migrationScriptFilename, testDefinitions);

            children.add(new FrameworkMethod(LiquiBaseMigrationScriptTestBase.class.getDeclaredMethod("allScriptsShouldBeTestedAndHavePreconditionsAndAllTestedFilesIncluded")));

        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected org.junit.runners.model.Statement classBlock(RunNotifier notifier) {
        setDatabaseMigrationFieldsOntoTest();
        return super.classBlock(notifier);
    }

    private void setDatabaseMigrationFieldsOntoTest() {
        TestClass testClass = getTestClass();
        Class<? extends LiquiBaseMigrationScriptTestBase> testBaseClass = (Class<? extends LiquiBaseMigrationScriptTestBase>) testClass.getJavaClass();

        try {
            Field databaseHelper = testBaseClass.getField("databaseHelper");
            databaseHelper.set(null, this.databaseHelper);

            Field schemaSnapshot = testBaseClass.getField("schemaSnapshot");
            schemaSnapshot.set(null, this.schemaSnapshot);

            Field migrationScriptsVerifier = testBaseClass.getField("migrationScriptsVerifier");
            migrationScriptsVerifier.set(null, this.migrationScriptsVerifier);

        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<FrameworkMethod> getChildren() {
        return children;
    }

    private class LiquiBaseMigrationScriptTesterFrameworkMethod extends FrameworkMethod {

        private final LiquiBaseMigrationTestDefinition definition;

        public LiquiBaseMigrationScriptTesterFrameworkMethod(LiquiBaseMigrationTestDefinition definition) throws NoSuchMethodException {
            super(LiquiBaseMigrationScriptTestBase.class.getDeclaredMethod("testDefinitionMigration"));
            this.definition = definition;
        }

        @Override
        public Object invokeExplosively(Object target, Object... params) throws Throwable {
            ((LiquiBaseMigrationScriptTestBase) target).definition = definition;
            return super.invokeExplosively(target, params);
        }

        @Override
        public String getName() {
            return definition.getMigrationName();
        }

        @Override
        public boolean equals(Object other) {
            return EqualsBuilder.reflectionEquals(this, other);
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }
    }
}
