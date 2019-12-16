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
package com.tyro.oss.dbevolution.assertions;

import liquibase.datatype.core.BlobType;
import liquibase.datatype.core.ClobType;
import liquibase.datatype.core.TinyIntType;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Types;
import java.time.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tyro.oss.dbevolution.assertions.ColumnAssert.StandardStringColumnAssertions.DEFAULT_VARCHAR_MAX_LENGTH;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;

public class ColumnAssert {

    private static final String ID_COLUMN_NAME = "id";
    private final TableAssert table;
    private final String name;
    private final Map<Class<?>, ColumnTypeAssertions> columnTypeAssertions = new HashMap<>();
    private final Database schema;
    private NullCheckColumnType columnType;
    private boolean disableNullCheck;

    public ColumnAssert(Database schema, TableAssert tableAssert, String columnName) {
        this.schema = schema;
        this.table = tableAssert;
        this.name = columnName;
        createTypeAssertions(schema);
    }

    private static void assertColumnSize(Database database, String tableName, String columnName, int columnSize) {
        Table table = database.findTable(tableName);
        assertEquals(columnSize, table.findColumn(columnName).getSizeAsInt(),
                format("Size of %s.%s", tableName, columnName));
    }

    private static void assertColumnMinimalSize(Database database, String tableName, String columnName, int columnSize) {
        Table table = database.findTable(tableName);
        int actualColumnSize = table.findColumn(columnName).getSizeAsInt();
        assertTrue(actualColumnSize >= columnSize,
                format("Size of %s.%s should be at least %d, but was %s.", tableName, columnName, columnSize, actualColumnSize));
    }

    private static void assertColumnScale(Database database, String tableName, String columnName, int columnScale) {
        Table table = database.findTable(tableName);
        assertEquals(columnScale, table.findColumn(columnName).getScale(),
                format("Scale of %s.%s", tableName, columnName));
    }

    private static void assertColumnIsTypeAndSize(Database database, String tableName, String columnName, int type, int columnSize) {
        assertColumnIsType(database, tableName, columnName, type);
        assertColumnSize(database, tableName, columnName, columnSize);
    }

    private static void assertColumnIsType(Database database, String tableName, String columnName, int sqlType) {
        Column column = database.findTable(tableName).findColumn(columnName);
        assertNotNull(column,
                format("Column '%s' does not exist.", columnName));
        assertEquals(sqlTypeToString(sqlType), sqlTypeToString(column.getTypeCode()),
                format("Type of %s.%s", tableName, columnName));
    }

    private static void assertColumnIsType(Database database, String tableName, String columnName, int... sqlTypes) {
        Column column = database.findTable(tableName).findColumn(columnName);
        assertNotNull(column,
                format("Column '%s' does not exist.", columnName));

        String actualType = sqlTypeToString(column.getTypeCode());

        List<String> expectedTypes = stream(sqlTypes)
                .mapToObj(ColumnAssert::sqlTypeToString)
                .collect(toList());

        assertTrue(expectedTypes.contains(actualType),
                format("Type of %s.%s should be one of %s, but was %s.", tableName, columnName, expectedTypes, actualType));
    }

    private static String sqlTypeToString(int sqlType) {
        try {
            for (Field field : Types.class.getFields()) {
                if ((Integer) field.get(null) == sqlType) {
                    return field.getName();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while accessing members of javax.sql.Types", e);
        }
        throw new IllegalArgumentException("Unknown SQL Type: " + sqlType);
    }

    private static void assertColumnIsLongType(Database database, String tableName, String columnName) {
        assertColumnIsType(database, tableName, columnName, Types.BIGINT);
        assertColumnSize(database, tableName, columnName, 19);
    }

    private static void assertColumnIsLocalDateTimeType(Database database, String tableName, String columnName) {
        assertColumnIsType(database, tableName, columnName, Types.TIMESTAMP);
    }

    private static void assertColumnIsLocalDateType(Database database, String tableName, String columnName) {
        assertColumnIsType(database, tableName, columnName, Types.DATE);
    }

    private static void assertColumnIsZoneIdType(Database database, String tableName, String columnName) {
        assertColumnIsTypeAndSize(database, tableName, columnName, Types.VARCHAR, 255);
    }

    private static void assertColumnIsNullable(Database database, String tableName, String columnName) {
        Table table = database.findTable(tableName);
        assertFalse(table.findColumn(columnName).isRequired(),
                format("%s.%s should be nullable", tableName, columnName));
    }

    private static void assertColumnIsNotNullable(Database database, String tableName, String columnName) {
        Table table = database.findTable(tableName);
        Column column = table.findColumn(columnName);
        assertTrue(column.isRequired(),
                format("%s.%s should be NOT NULL", tableName, columnName));
    }

    private static void assertColumnHasDefaultValue(Database database, String tableName, String columnName, Object defaultValue) {
        Column column = database.findTable(tableName).findColumn(columnName);
        Object parsedDefaultValue = column.getParsedDefaultValue();
        if (defaultValue != null) {
            assertEquals(defaultValue, parsedDefaultValue);
            assertEquals(defaultValue.getClass().getName(), parsedDefaultValue.getClass().getName());
        } else {
            assertNull(parsedDefaultValue);
        }
    }

    private static void assertColumnIsTypeWithSizeAndScale(Database database, String tableName, String columnName, int sqlType, int size, int scale) {
        assertColumnIsType(database, tableName, columnName, sqlType);
        assertColumnSize(database, tableName, columnName, size);
        assertColumnScale(database, tableName, columnName, scale);
    }

    private void createTypeAssertions(Database schema) {
        columnTypeAssertions.put(Boolean.class, new BooleanColumnAssertions(schema));
        columnTypeAssertions.put(Boolean.TYPE, new BooleanColumnAssertions(schema));
        columnTypeAssertions.put(Character.class, new CharacterColumnAssertions(1));
        columnTypeAssertions.put(Character.TYPE, new CharacterColumnAssertions(1));
        columnTypeAssertions.put(Enum.class, new StandardEnumColumnAssertions());
        columnTypeAssertions.put(Integer.class, new IntegerColumnAssertions(schema));
        columnTypeAssertions.put(Integer.TYPE, new IntegerColumnAssertions(schema));
        columnTypeAssertions.put(Double.class, new DoubleColumnAssertions());
        columnTypeAssertions.put(Double.TYPE, new DoubleColumnAssertions());
        columnTypeAssertions.put(Long.class, new LongColumnAssertions());
        columnTypeAssertions.put(Long.TYPE, new LongColumnAssertions());
        columnTypeAssertions.put(BigDecimal.class, new BigDecimalColumnAssertions());
        columnTypeAssertions.put(YearMonth.class, new YearMonthColumnAssertions(schema));
        columnTypeAssertions.put(LocalDate.class, new DateCompatibleColumnAssertions());
        columnTypeAssertions.put(LocalTime.class, new LocalTimeColumnAssertions());
        columnTypeAssertions.put(LocalDateTime.class, new DateTimeColumnAssertions());
        columnTypeAssertions.put(ZoneId.class, new ZoneIdColumnAssertions());
        columnTypeAssertions.put(String.class, new StandardStringColumnAssertions());
        columnTypeAssertions.put(byte[].class, new BinaryColumnAssertions(schema));
        columnTypeAssertions.put(TinyIntType.class, new TinyIntColumnAssertions(3));
        columnTypeAssertions.put(BlobType.class, new BlobColumnAssertions(schema));
        columnTypeAssertions.put(ClobType.class, new ClobColumnAssertions(schema));
    }

    public ColumnAssert isNotPresent() {
        assertColumnNotInTable(schema, table.getName(), name);
        return this;
    }

    public ColumnAssert isPresent() {
        assertColumnInTable(schema, table.getName(), name);
        return this;
    }

    private void assertColumnInTable(Database database, String tableName, String columnName) {
        Table table = database.findTable(tableName);
        assertNotNull(table.findColumn(columnName),
                format("Column '%s' not present in '%s.%s'", columnName, database.getName(), tableName));
    }

    private void assertColumnNotInTable(Database database, String tableName, String columnName) {
        Table table = database.findTable(tableName);
        assertNull(table.findColumn(columnName),
                format("Column %s.%s should not exist", tableName, columnName));
    }

    public ColumnAssert supportsType(Class<?> hibernateFieldType) {
        ColumnTypeAssertions assertions = columnTypeAssertions.get(hibernateFieldType);
        if (assertions == null) {
            throw new IllegalArgumentException("No SQL Type Assertions have been defined supporting " + hibernateFieldType.getName());
        }
        assertions.performAssertions(schema, table.getName(), name);
        return this;
    }

    public ColumnAssert supportsString(int minimalSize) {
        assert minimalSize >= DEFAULT_VARCHAR_MAX_LENGTH : "There is no storage saving in having a varchar less than 255 in length";
        TextualColumnAssertions textualColumnAssertions = TextualColumnAssertions.buildTextualColumnAssertions(schema).withMinimalSize(minimalSize).allowClobs().build();
        textualColumnAssertions.performAssertions(schema, table.getName(), name);
        return this;
    }

    public ColumnAssert supportsDecimal(int size, int scale) {
        assertColumnIsTypeWithSizeAndScale(schema, table.getName(), name, Types.DECIMAL, size, scale);
        return this;
    }

    public ColumnAssert supportsDataTypeWithSizeAndScale(int dataType, int size, int scale) {
        assertColumnIsTypeWithSizeAndScale(schema, table.getName(), name, dataType, size, scale);
        return this;
    }

    public ColumnAssert supportsFixedWidthCharacter(int size) {
        assert size > 0 : "Character column size must be greater than zero";
        assert size < 255 : "Use a varchar type for strings greater than or equal to 255 characters!";
        CharacterColumnAssertions columnAssertions = new CharacterColumnAssertions(size);
        columnAssertions.performAssertions(schema, table.getName(), name);
        return this;
    }

    public ColumnAssert supportsLongString(int size) {
        assert size > DEFAULT_VARCHAR_MAX_LENGTH : "There is no storage saving in having a varchar less than 255 in length";
        new StringColumnAssertions(size).performAssertions(schema, table.getName(), name);
        return this;
    }

    public ColumnAssert supportsIdType() {
        return supportsType(Long.class);
    }

    public ColumnAssert isNotNullable() {
        assertColumnIsNotNullable(schema, table.getName(), name);

        Column column = schema.findTable(table.getName()).findColumn(name);

        boolean columnHasDefaultValue = column.getDefaultValue() != null;
        boolean columnAutoIncrement = column.isAutoIncrement();
        boolean nullCheckIsRequired = !isPreMigrationTest() && !table.isNewTable() && !disableNullCheck;

        if (!columnHasDefaultValue && !columnAutoIncrement && nullCheckIsRequired) {
            fail("Problem with column \"" + name
                    + "\". \n\tNew columns that are not in a new table must either be nullable or have a default "
                    + "value or be auto-increment.\nI\tf this is a new table, use TableAssert.enterNewTableAssertionMode().\n\tAlternatively, use "
                    + "ColumnAssert.disableNullCheck()");
        }

        return this;
    }

    private boolean isPreMigrationTest() {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        for (StackTraceElement stackTraceElement : stack) {
            if (stackTraceElement.getMethodName().equals("assertPreMigrationSchema")) {
                return true;
            }
        }
        return false;
    }

    public ColumnAssert isNullable() {
        assertColumnIsNullable(schema, table.getName(), name);
        return this;
    }

    public ColumnAssert withDefaultValueOf(Object defaultValue) {
        assertColumnHasDefaultValue(schema, table.getName(), name, defaultValue);
        return this;
    }

    public ColumnAssert isNonAutoIncrementingPrimaryKey() {
        return supportsIdType()
                .isNotNullable()
                .isNotAutoIncrementing()
                .isPrimaryKey();
    }

    public ColumnAssert isPrimaryKeyIdColumn() {
        return supportsIdType()
                .isNotNullable()
                .isAutoIncrementing()
                .isPrimaryKey();
    }

    public ColumnAssert isPrimaryKey() {
        String[] primaryKeyColumnNames = new String[]{name};
        Table table1 = schema.findTable(table.getName());
        int numberPrimaryKeys = 0;
        for (int i = 0; i < table1.getColumns().length; i++) {
            String columnName = table1.getColumns()[i].getName();
            if (asList(primaryKeyColumnNames).contains(columnName)) {
                assertTrue(table1.findColumn(columnName).isPrimaryKey(),
                        format("%s should be a primary key member", columnName));
                numberPrimaryKeys++;
            }
        }
        assertEquals(primaryKeyColumnNames.length, numberPrimaryKeys,
                format("Number of columns in %sprimary key", table.getName()));
        return this;
    }

    public ColumnAssert isAutoIncrementing() {
        Column column = schema.findTable(table.getName()).findColumn(name);
        assertNotNull(column,
                format("Column '%s' does not exist.", name));
        assertTrue(column.isAutoIncrement(),
                format("%s.%s should be auto-increment", table.getName(), name));
        return this;
    }

    public ColumnAssert isNotAutoIncrementing() {
        Column column = schema.findTable(table.getName()).findColumn(name);
        assertNotNull(column,
                format("Column '%s' does not exist.", name));
        assertFalse(column.isAutoIncrement(),
                format("%s.%s should not be auto-increment", table.getName(), name));
        return this;
    }

    public ForeignKeyAssert hasForeignKeyTo(String targetTable) {
        return new ForeignKeyAssert(schema, this, targetTable, ID_COLUMN_NAME).isPresent();
    }

    String getName() {
        return name;
    }

    public ForeignKeyAssert hasForeignKeyTo(String targetTable, String targetColumn) {
        assertNotEquals(ID_COLUMN_NAME, targetColumn,
                format("use hasForeignKeyTo(String targetTable) when the target column is \"%s\"", ID_COLUMN_NAME));
        return new ForeignKeyAssert(schema, this, targetTable, targetColumn).isPresent();
    }

    public ColumnAssert doesNotHaveForeignKeyTo(String targetTable) {
        return new ForeignKeyAssert(schema, this, targetTable, ID_COLUMN_NAME).isNotPresent().andColumn();
    }

    public ColumnAssert doesNotHaveForeignKeyTo(String targetTable, String targetColumn) {
        assertNotEquals(ID_COLUMN_NAME, targetColumn,
                format("use doesNotHaveForeignKeyTo(String targetTable) when the target column is \"%s\"", ID_COLUMN_NAME));
        return new ForeignKeyAssert(schema, this, targetTable, targetColumn).isNotPresent().andColumn();
    }

    public TableAssert andTable() {
        return table;
    }

    public ColumnAssert hasColumn(String columnName) {
        return andTable().hasColumn(columnName);
    }

    public DisableNullCheckExplanation disableNullCheck(NullCheckColumnType columnType) {
        this.columnType = columnType;
        return new DisableNullCheckExplanation(this);
    }

    public enum NullCheckColumnType {
        EXISTING_COLUMN,
        NEW_COLUMN
    }

    public enum DisableNullCheckReason {
        CODE_CURRENTLY_IN_PRODUCTION_DOES_NOT_WRITE_NULL_AND_COLUMN_CONTAINS_NO_NULL_VALUES,
        CODE_CURRENTLY_IN_PRODUCTION_DOES_NOT_WRITE_NULL_AND_SCRIPT_ELIMINATES_EXISTING_NULLS,
        TABLE_IS_EMPTY_IN_PRODUCTION_AND_WILL_REMAIN_EMPTY_UNTIL_THIS_IS_DEPLOYED,
        COLUMN_IS_NOT_NULL_AND_SHOULD_STAY_THAT_WAY,
        TABLE_NOT_IN_PRODUCTION
    }

    public static class DisableNullCheckExplanation {

        private final ColumnAssert columnAssert;

        DisableNullCheckExplanation(ColumnAssert columnAssert) {
            this.columnAssert = columnAssert;
        }

        public DisableNullCheckAuthorisation reason(DisableNullCheckReason reason) {
            assertNotNull(reason);
            if (columnAssert.columnType == NullCheckColumnType.NEW_COLUMN
                    && reason != DisableNullCheckReason.TABLE_IS_EMPTY_IN_PRODUCTION_AND_WILL_REMAIN_EMPTY_UNTIL_THIS_IS_DEPLOYED
                    && reason != DisableNullCheckReason.TABLE_NOT_IN_PRODUCTION) {
                fail("You must not disable the null check for a new column in a non-empty table, because this will break if the code is rolled back.\n"
                        + "New columns in existing tables should preferably be nullable, but can have a default value if there is a SENSIBLE default (not a token value).");
            }
            return new DisableNullCheckAuthorisation(this.columnAssert);
        }
    }

    public static class DisableNullCheckAuthorisation {

        private final ColumnAssert columnAssert;

        DisableNullCheckAuthorisation(ColumnAssert columnAssert) {
            this.columnAssert = columnAssert;
        }

        public ColumnAssert checkedBy(String... developers) {
            columnAssert.disableNullCheck = true;
            return this.columnAssert;
        }
    }

    private static class IntegerColumnAssertions extends ColumnTypeAssertions {

        IntegerColumnAssertions(Database schema) {
            super(Types.INTEGER, 10);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsTypeAndSize(schema, tableName, columnName, Types.INTEGER, 10);
        }
    }

    private static class LocalTimeColumnAssertions extends ColumnTypeAssertions {

        LocalTimeColumnAssertions() {
            super(Types.TIME);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(schema, tableName, columnName, Types.TIME);
        }
    }

    private static class LongColumnAssertions extends ColumnTypeAssertions {

        LongColumnAssertions() {
            super(Types.BIGINT, 19);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsLongType(schema, tableName, columnName);
        }
    }

    private static class StandardEnumColumnAssertions extends ColumnTypeAssertions {

        StandardEnumColumnAssertions() {
            super(Types.CHAR);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(schema, tableName, columnName, Types.CHAR);
        }
    }

    public static class StandardStringColumnAssertions extends StringColumnAssertions {

        public static final int DEFAULT_VARCHAR_MAX_LENGTH = 255;

        StandardStringColumnAssertions() {
            super(DEFAULT_VARCHAR_MAX_LENGTH);
        }
    }

    private static class StringColumnAssertions extends ColumnTypeAssertions {

        private final boolean acceptBiggerSize;

        StringColumnAssertions(int expectedSize) {
            this(expectedSize, false);
        }

        public StringColumnAssertions(int expectedSize, boolean acceptBiggerSize) {
            super(Types.VARCHAR, expectedSize);
            this.acceptBiggerSize = acceptBiggerSize;
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(schema, tableName, columnName, Types.VARCHAR);
            if (acceptBiggerSize) {
                assertColumnMinimalSize(schema, tableName, columnName, expectedSize);
            } else {
                assertColumnSize(schema, tableName, columnName, expectedSize);
            }
        }
    }

    private static class TinyIntColumnAssertions extends ColumnTypeAssertions {

        TinyIntColumnAssertions(int expectedSize) {
            super(Types.TINYINT, expectedSize);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(schema, tableName, columnName, Types.TINYINT);
            assertColumnSize(schema, tableName, columnName, expectedSize);
        }
    }

    private static class TextualColumnAssertions extends ColumnTypeAssertions {

        private final static int[] ALLOWED_TYPES = {Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.CLOB, Types.NCLOB};
        private final static int[] ALLOWED_TYPES_NO_CLOB = {Types.VARCHAR, Types.NVARCHAR};

        private final Database schema;
        private final boolean acceptBiggerSize;
        private final boolean allowClobs;

        private TextualColumnAssertions(Database schema, int expectedSize, boolean acceptBiggerSize, boolean allowClobs) {
            super(Types.VARCHAR, expectedSize);
            this.schema = schema;
            this.acceptBiggerSize = acceptBiggerSize;
            this.allowClobs = allowClobs;
        }

        public static Builder buildTextualColumnAssertions(Database schema) {
            return new Builder(schema);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            if (allowClobs) {
                assertColumnIsType(this.schema, tableName, columnName, ALLOWED_TYPES);
            } else {
                assertColumnIsType(this.schema, tableName, columnName, ALLOWED_TYPES_NO_CLOB);
            }
            if (acceptBiggerSize) {
                assertColumnMinimalSize(this.schema, tableName, columnName, expectedSize);
            } else {
                assertColumnSize(this.schema, tableName, columnName, expectedSize);
            }
        }

        private static class Builder {

            private final Database schema;
            private int size = 255;
            private boolean acceptBiggerSize = false;
            private boolean allowClobs = false;

            private Builder(Database schema) {
                this.schema = schema;
            }

            public Builder withSize(int size) {
                this.acceptBiggerSize = false;
                this.size = size;
                return this;
            }

            public Builder withMinimalSize(int size) {
                this.acceptBiggerSize = true;
                this.size = size;
                return this;
            }

            public Builder restrictToVarchar() {
                this.allowClobs = false;
                return this;
            }

            public Builder allowClobs() {
                this.allowClobs = true;
                return this;
            }

            public TextualColumnAssertions build() {
                return new TextualColumnAssertions(schema, size, acceptBiggerSize, allowClobs);
            }
        }
    }

    private static class YearMonthColumnAssertions extends ColumnTypeAssertions {

        private final Database schema;

        YearMonthColumnAssertions(Database schema) {
            super(Types.CHAR, 4);
            this.schema = schema;
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(this.schema, tableName, columnName, Types.CHAR);
            assertColumnSize(this.schema, tableName, columnName, 4);
        }
    }

    private static class ZoneIdColumnAssertions extends ColumnTypeAssertions {

        ZoneIdColumnAssertions() {
            super(Types.VARCHAR);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsZoneIdType(schema, tableName, columnName);
        }
    }

    private static class BigDecimalColumnAssertions extends ColumnTypeAssertions {

        private BigDecimalColumnAssertions() {
            super(Types.DECIMAL);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(schema, tableName, columnName);
        }
    }

    private static class DoubleColumnAssertions extends ColumnTypeAssertions {

        DoubleColumnAssertions() {
            super(Types.DOUBLE, 16, 10);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsTypeAndSize(schema, tableName, columnName, Types.DOUBLE, 16);
        }
    }

    private static class DateTimeColumnAssertions extends ColumnTypeAssertions {

        DateTimeColumnAssertions() {
            super(Types.TIMESTAMP);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsLocalDateTimeType(schema, tableName, columnName);
        }
    }

    private static class DateCompatibleColumnAssertions extends ColumnTypeAssertions {

        DateCompatibleColumnAssertions() {
            super(Types.DATE);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsLocalDateType(schema, tableName, columnName);
        }
    }

    private static class ClobColumnAssertions extends ColumnTypeAssertions {

        private Database schema;

        ClobColumnAssertions(Database schema) {
            super(Types.LONGVARCHAR);
            this.schema = schema;
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(this.schema, tableName, columnName, Types.LONGVARCHAR);
        }
    }

    private static class CharacterColumnAssertions extends ColumnTypeAssertions {

        CharacterColumnAssertions(int expectedSize) {
            super(Types.CHAR, expectedSize);
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(schema, tableName, columnName, Types.CHAR);
            assertColumnSize(schema, tableName, columnName, expectedSize);
        }
    }

    private static class BooleanColumnAssertions extends ColumnTypeAssertions {

        private final Database schema;

        BooleanColumnAssertions(Database schema) {
            super(Types.BIT, 1);
            this.schema = schema;
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(this.schema, tableName, columnName, Types.BIT);
            assertColumnSize(this.schema, tableName, columnName, 1);
        }
    }

    private static class BlobColumnAssertions extends ColumnTypeAssertions {

        private Database schema;

        BlobColumnAssertions(Database schema) {
            super(Types.LONGVARBINARY);
            this.schema = schema;
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(this.schema, tableName, columnName, Types.LONGVARBINARY);
        }
    }

    private static class BinaryColumnAssertions extends ColumnTypeAssertions {

        private Database schema;

        BinaryColumnAssertions(Database schema) {
            super(Types.BINARY);
            this.schema = schema;
        }

        @Override
        protected void makeAssertions(Database schema, String tableName, String columnName) {
            assertColumnIsType(this.schema, tableName, columnName, Types.BINARY);
        }
    }

    public abstract static class ColumnTypeAssertions {

        protected final int expectedType;
        protected final Integer expectedSize;
        protected final Integer expectedScale;

        public ColumnTypeAssertions(int expectedType) {
            this.expectedType = expectedType;
            this.expectedSize = null;
            this.expectedScale = null;
        }

        public ColumnTypeAssertions(int expectedType, int expectedSize) {
            this.expectedType = expectedType;
            this.expectedSize = expectedSize;
            this.expectedScale = null;
        }

        public ColumnTypeAssertions(int expectedType, int expectedSize, int expectedScale) {
            this.expectedType = expectedType;
            this.expectedSize = expectedSize;
            this.expectedScale = expectedScale;
        }

        public final void performAssertions(Database schema, String tableName, String columnName) {
            makeAssertions(schema, tableName, columnName);
        }

        protected abstract void makeAssertions(Database schema, String tableName, String columnName);
    }
}
