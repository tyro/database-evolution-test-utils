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

import com.tyro.oss.dbevolution.database.DatabaseCleaner;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.transaction.annotation.AnnotationTransactionAttributeSource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttributeSource;

import java.lang.reflect.Method;
import java.util.EnumSet;

import static java.util.Arrays.stream;
import static java.util.EnumSet.complementOf;
import static org.springframework.transaction.annotation.Propagation.*;

public class DatabaseMigrationTestExecutionListener extends AbstractTestExecutionListener {

    private static final EnumSet<Propagation> PROPAGATION_TYPES_THAT_REQUIRE_DATABASE_CLEANING = complementOf(EnumSet.of(REQUIRED, REQUIRES_NEW, MANDATORY));
    private final DatabaseCleaner databaseCleaner;

    public DatabaseMigrationTestExecutionListener(DatabaseDetails databaseDetails,
                                                  String snapshotFilename,
                                                  String migrationScriptsFilename) {
        this.databaseCleaner = new DatabaseCleaner(databaseDetails, snapshotFilename, migrationScriptsFilename);
    }

    @Override
    public void beforeTestClass(TestContext testContext) {
        databaseCleaner.cleanAndMigrateSchemaAndGenesis();
    }

    @Override
    public void beforeTestMethod(TestContext testContext) {
        databaseCleaner.cleanAndMigrateSchemaAndGenesis();
    }

    @Override
    public void afterTestMethod(TestContext testContext) throws Exception {
        TransactionAttributeSource attributeSource = new AnnotationTransactionAttributeSource();

        Class<?> testClass;
        Method testMethod;

        if (TestContext.class.isInterface()) {
            Class<?> testContextClass = testContext.getClass();
            Method getTestMethodMethod = testContextClass.getMethod("getTestMethod");
            getTestMethodMethod.setAccessible(true);
            testMethod = (Method) getTestMethodMethod.invoke(testContext);

            Method getTestClassMethod = testContextClass.getMethod("getTestClass");
            getTestClassMethod.setAccessible(true);
            testClass = (Class<?>) getTestClassMethod.invoke(testContext);
        } else {
            testMethod = testContext.getTestMethod();
            testClass = testContext.getTestClass();
        }

        TransactionAttribute transactionAttribute = attributeSource.getTransactionAttribute(testMethod, testClass);
        markDatabaseAsNeedingToBeReloadedIfRequired(transactionAttribute);
    }

    private void markDatabaseAsNeedingToBeReloadedIfRequired(TransactionAttribute transactionAttribute) {
        if (transactionAttribute == null || PROPAGATION_TYPES_THAT_REQUIRE_DATABASE_CLEANING.contains(convert(transactionAttribute.getPropagationBehavior()))) {
            databaseCleaner.markDatabaseAsNeedingToBeReloaded();
        }
    }

    private Propagation convert(int propagationBehavior) {
        return stream(values())
                .filter(propagation -> propagation.value() == propagationBehavior)
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Unexpected Propagation " + propagationBehavior));
    }
}
