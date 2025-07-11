/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Column.PhysicalColumn;
import org.apache.flink.table.catalog.Constraint;
import org.apache.flink.table.catalog.Index;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Schema of a table or view.
 *
 * <p>A schema represents the schema part of a {@code CREATE TABLE (schema) WITH (options)} DDL
 * statement in SQL. It defines columns of different kinds, constraints, indexes, time attributes,
 * and watermark strategies. It is possible to reference objects (such as functions or types) across
 * different catalogs.
 *
 * <p>This class is used in the API and catalogs to define an unresolved schema that will be
 * translated to {@link ResolvedSchema}. Some methods of this class perform basic validation,
 * however, the main validation happens during the resolution. Thus, an unresolved schema can be
 * incomplete and might be enriched or merged with a different schema at a later stage.
 *
 * <p>Since an instance of this class is unresolved, it should not be directly persisted. The {@link
 * #toString()} shows only a summary of the contained objects.
 */
@PublicEvolving
public final class Schema {

    private static final Schema EMPTY = Schema.newBuilder().build();

    private final List<UnresolvedColumn> columns;

    private final List<UnresolvedWatermarkSpec> watermarkSpecs;

    private final @Nullable UnresolvedPrimaryKey primaryKey;

    private final List<UnresolvedIndex> indexes;

    /** Please use {@link #Schema(List, List, UnresolvedPrimaryKey, List)} instead. */
    @Deprecated
    public Schema(
            List<UnresolvedColumn> columns,
            List<UnresolvedWatermarkSpec> watermarkSpecs,
            @Nullable UnresolvedPrimaryKey primaryKey) {
        this(columns, watermarkSpecs, primaryKey, Collections.emptyList());
    }

    public Schema(
            List<UnresolvedColumn> columns,
            List<UnresolvedWatermarkSpec> watermarkSpecs,
            @Nullable UnresolvedPrimaryKey primaryKey,
            List<UnresolvedIndex> indexes) {
        this.columns = Collections.unmodifiableList(columns);
        this.watermarkSpecs = Collections.unmodifiableList(watermarkSpecs);
        this.primaryKey = primaryKey;
        this.indexes = Collections.unmodifiableList(indexes);
    }

    /** Builder for configuring and creating instances of {@link Schema}. */
    public static Schema.Builder newBuilder() {
        return new Builder();
    }

    /**
     * Convenience method for stating explicitly that a schema is empty and should be fully derived
     * by the framework.
     *
     * <p>The semantics are equivalent to calling {@code Schema.newBuilder().build()}.
     *
     * <p>Note that derivation depends on the context. Usually, the method that accepts a {@link
     * Schema} instance will mention whether schema derivation is supported or not.
     */
    public static Schema derived() {
        return EMPTY;
    }

    public List<UnresolvedColumn> getColumns() {
        return columns;
    }

    public List<UnresolvedWatermarkSpec> getWatermarkSpecs() {
        return watermarkSpecs;
    }

    public Optional<UnresolvedPrimaryKey> getPrimaryKey() {
        return Optional.ofNullable(primaryKey);
    }

    public List<UnresolvedIndex> getIndexes() {
        return indexes;
    }

    /** Resolves the given {@link Schema} to a validated {@link ResolvedSchema}. */
    public ResolvedSchema resolve(SchemaResolver resolver) {
        return resolver.resolve(this);
    }

    @Override
    public String toString() {
        final List<Object> components = new ArrayList<>();
        components.addAll(columns);
        components.addAll(watermarkSpecs);
        if (primaryKey != null) {
            components.add(primaryKey);
        }
        if (!indexes.isEmpty()) {
            components.addAll(indexes);
        }
        return components.stream()
                .map(Objects::toString)
                .map(s -> "  " + s)
                .collect(Collectors.joining(",\n", "(\n", "\n)"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Schema schema = (Schema) o;
        return columns.equals(schema.columns)
                && watermarkSpecs.equals(schema.watermarkSpecs)
                && Objects.equals(primaryKey, schema.primaryKey)
                && indexes.equals(schema.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, watermarkSpecs, primaryKey, indexes);
    }

    // --------------------------------------------------------------------------------------------

    /** A builder for constructing an immutable but still unresolved {@link Schema}. */
    @PublicEvolving
    public static final class Builder {

        private final List<UnresolvedColumn> columns;

        private final List<UnresolvedWatermarkSpec> watermarkSpecs;

        private @Nullable UnresolvedPrimaryKey primaryKey;

        private final List<UnresolvedIndex> indexes;

        private Builder() {
            columns = new ArrayList<>();
            watermarkSpecs = new ArrayList<>();
            indexes = new ArrayList<>();
        }

        /** Adopts all members from the given unresolved schema. */
        public Builder fromSchema(Schema unresolvedSchema) {
            columns.addAll(unresolvedSchema.columns);
            watermarkSpecs.addAll(unresolvedSchema.watermarkSpecs);
            if (unresolvedSchema.primaryKey != null) {
                primaryKeyNamed(
                        unresolvedSchema.primaryKey.getConstraintName(),
                        unresolvedSchema.primaryKey.getColumnNames());
            }
            indexes.addAll(unresolvedSchema.indexes);
            return this;
        }

        /** Adopts all members from the given resolved schema. */
        public Builder fromResolvedSchema(ResolvedSchema resolvedSchema) {
            addResolvedColumns(resolvedSchema.getColumns());
            addResolvedWatermarkSpec(resolvedSchema.getWatermarkSpecs());
            resolvedSchema.getPrimaryKey().ifPresent(this::addResolvedConstraint);
            addResolvedIndexes(resolvedSchema.getIndexes());
            return this;
        }

        /** Adopts all fields of the given row as physical columns of the schema. */
        public Builder fromRowDataType(DataType dataType) {
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            Preconditions.checkArgument(
                    dataType.getLogicalType().is(LogicalTypeRoot.ROW),
                    "Data type of ROW expected.");
            final List<DataType> fieldDataTypes = dataType.getChildren();
            final List<String> fieldNames = ((RowType) dataType.getLogicalType()).getFieldNames();
            IntStream.range(0, fieldDataTypes.size())
                    .forEach(i -> column(fieldNames.get(i), fieldDataTypes.get(i)));
            return this;
        }

        /** Adopts the given field names and field data types as physical columns of the schema. */
        public Builder fromFields(String[] fieldNames, AbstractDataType<?>[] fieldDataTypes) {
            Preconditions.checkNotNull(fieldNames, "Field names must not be null.");
            Preconditions.checkNotNull(fieldDataTypes, "Field data types must not be null.");
            Preconditions.checkArgument(
                    fieldNames.length == fieldDataTypes.length,
                    "Field names and field data types must have the same length.");
            IntStream.range(0, fieldNames.length)
                    .forEach(i -> column(fieldNames[i], fieldDataTypes[i]));
            return this;
        }

        /** Adopts the given field names and field data types as physical columns of the schema. */
        public Builder fromFields(
                List<String> fieldNames, List<? extends AbstractDataType<?>> fieldDataTypes) {
            Preconditions.checkNotNull(fieldNames, "Field names must not be null.");
            Preconditions.checkNotNull(fieldDataTypes, "Field data types must not be null.");
            Preconditions.checkArgument(
                    fieldNames.size() == fieldDataTypes.size(),
                    "Field names and field data types must have the same length.");
            IntStream.range(0, fieldNames.size())
                    .forEach(i -> column(fieldNames.get(i), fieldDataTypes.get(i)));
            return this;
        }

        /** Adopts all columns from the given list. */
        public Builder fromColumns(List<UnresolvedColumn> unresolvedColumns) {
            columns.addAll(unresolvedColumns);
            return this;
        }

        /**
         * Declares a physical column that is appended to this schema.
         *
         * <p>Physical columns are regular columns known from databases. They define the names, the
         * types, and the order of fields in the physical data. Thus, physical columns represent the
         * payload that is read from and written to an external system. Connectors and formats use
         * these columns (in the defined order) to configure themselves. Other kinds of columns can
         * be declared between physical columns but will not influence the final physical schema.
         *
         * @param columnName column name
         * @param dataType data type of the column
         */
        public Builder column(String columnName, AbstractDataType<?> dataType) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedPhysicalColumn(columnName, dataType));
            return this;
        }

        /**
         * Declares a physical column that is appended to this schema.
         *
         * <p>See {@link #column(String, AbstractDataType)} for a detailed explanation.
         *
         * <p>This method uses a type string that can be easily persisted in a durable catalog.
         *
         * @param columnName column name
         * @param serializableTypeString data type of the column as a serializable string
         * @see LogicalType#asSerializableString()
         */
        public Builder column(String columnName, String serializableTypeString) {
            return column(columnName, DataTypes.of(serializableTypeString));
        }

        /**
         * Declares a computed column that is appended to this schema.
         *
         * <p>Computed columns are virtual columns that are generated by evaluating an expression
         * that can reference other columns declared in the same table. Both physical columns and
         * metadata columns can be accessed. The column itself is not physically stored within the
         * table. The column’s data type is derived automatically from the given expression and does
         * not have to be declared manually.
         *
         * <p>Computed columns are commonly used for defining time attributes. For example, the
         * computed column can be used if the original field is not TIMESTAMP(3) type or is nested
         * in a JSON string.
         *
         * <p>Any scalar expression can be used for in-memory/temporary tables. However, currently,
         * only SQL expressions can be persisted in a catalog. User-defined functions (also defined
         * in different catalogs) are supported.
         *
         * <p>Example: {@code .columnByExpression("ts", $("json_obj").get("ts").cast(TIMESTAMP(3))}
         *
         * @param columnName column name
         * @param expression computation of the column
         */
        public Builder columnByExpression(String columnName, Expression expression) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(expression, "Expression must not be null.");
            columns.add(new UnresolvedComputedColumn(columnName, expression));
            return this;
        }

        /**
         * Declares a computed column that is appended to this schema.
         *
         * <p>See {@link #columnByExpression(String, Expression)} for a detailed explanation.
         *
         * <p>This method uses a SQL expression that can be easily persisted in a durable catalog.
         *
         * <p>Example: {@code .columnByExpression("ts", "CAST(json_obj.ts AS TIMESTAMP(3))")}
         *
         * @param columnName column name
         * @param sqlExpression computation of the column using SQL
         */
        public Builder columnByExpression(String columnName, String sqlExpression) {
            return columnByExpression(columnName, new SqlCallExpression(sqlExpression));
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. For convenience, the runtime will perform an explicit cast if the data type of the
         * column differs from the data type of the metadata field. Of course, this requires that
         * the two data types are compatible.
         *
         * <p>Note: This method assumes that the metadata key is equal to the column name and the
         * metadata column can be used for both reading and writing.
         *
         * @param columnName column name
         * @param dataType data type of the column
         */
        public Builder columnByMetadata(String columnName, AbstractDataType<?> dataType) {
            return columnByMetadata(columnName, dataType, null, false);
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>See {@link #column(String, AbstractDataType)} for a detailed explanation.
         *
         * <p>This method uses a type string that can be easily persisted in a durable catalog.
         *
         * @param columnName column name
         * @param serializableTypeString data type of the column
         */
        public Builder columnByMetadata(String columnName, String serializableTypeString) {
            return columnByMetadata(columnName, serializableTypeString, null, false);
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. For convenience, the runtime will perform an explicit cast if the data type of the
         * column differs from the data type of the metadata field. Of course, this requires that
         * the two data types are compatible.
         *
         * <p>By default, a metadata column can be used for both reading and writing. However, in
         * many cases an external system provides more read-only metadata fields than writable
         * fields. Therefore, it is possible to exclude metadata columns from persisting by setting
         * the {@code isVirtual} flag to {@code true}.
         *
         * <p>Note: This method assumes that the metadata key is equal to the column name.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param isVirtual whether the column should be persisted or not
         */
        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, boolean isVirtual) {
            return columnByMetadata(columnName, dataType, null, isVirtual);
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. The metadata key can be omitted if the column name should be used as the
         * identifying metadata key. For convenience, the runtime will perform an explicit cast if
         * the data type of the column differs from the data type of the metadata field. Of course,
         * this requires that the two data types are compatible.
         *
         * <p>Note: This method assumes that a metadata column can be used for both reading and
         * writing.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param metadataKey identifying metadata key, if null the column name will be used as
         *     metadata key
         */
        public Builder columnByMetadata(
                String columnName, AbstractDataType<?> dataType, @Nullable String metadataKey) {
            return columnByMetadata(columnName, dataType, metadataKey, false);
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>Metadata columns allow to access connector and/or format specific fields for every row
         * of a table. For example, a metadata column can be used to read and write the timestamp
         * from and to Kafka records for time-based operations. The connector and format
         * documentation lists the available metadata fields for every component.
         *
         * <p>Every metadata field is identified by a string-based key and has a documented data
         * type. The metadata key can be omitted if the column name should be used as the
         * identifying metadata key. For convenience, the runtime will perform an explicit cast if
         * the data type of the column differs from the data type of the metadata field. Of course,
         * this requires that the two data types are compatible.
         *
         * <p>By default, a metadata column can be used for both reading and writing. However, in
         * many cases an external system provides more read-only metadata fields than writable
         * fields. Therefore, it is possible to exclude metadata columns from persisting by setting
         * the {@code isVirtual} flag to {@code true}.
         *
         * @param columnName column name
         * @param dataType data type of the column
         * @param metadataKey identifying metadata key, if null the column name will be used as
         *     metadata key
         * @param isVirtual whether the column should be persisted or not
         */
        public Builder columnByMetadata(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(dataType, "Data type must not be null.");
            columns.add(new UnresolvedMetadataColumn(columnName, dataType, metadataKey, isVirtual));
            return this;
        }

        /**
         * Declares a metadata column that is appended to this schema.
         *
         * <p>See {@link #columnByMetadata(String, AbstractDataType, String, boolean)} for a
         * detailed explanation.
         *
         * <p>This method uses a type string that can be easily persisted in a durable catalog.
         *
         * @param columnName column name
         * @param serializableTypeString data type of the column
         * @param metadataKey identifying metadata key, if null the column name will be used as
         *     metadata key
         * @param isVirtual whether the column should be persisted or not
         */
        public Builder columnByMetadata(
                String columnName,
                String serializableTypeString,
                @Nullable String metadataKey,
                boolean isVirtual) {
            return columnByMetadata(
                    columnName, DataTypes.of(serializableTypeString), metadataKey, isVirtual);
        }

        /** Apply comment to the previous column. */
        public Builder withComment(@Nullable String comment) {
            if (columns.size() > 0) {
                columns.set(
                        columns.size() - 1, columns.get(columns.size() - 1).withComment(comment));
            } else {
                throw new IllegalArgumentException(
                        "Method 'withComment(...)' must be called after a column definition, "
                                + "but there is no preceding column defined.");
            }
            return this;
        }

        /**
         * Declares that the given column should serve as an event-time (i.e. rowtime) attribute and
         * specifies a corresponding watermark strategy as an expression.
         *
         * <p>The column must be of type {@code TIMESTAMP(3)} or {@code TIMESTAMP_LTZ(3)} and be a
         * top-level column in the schema. It may be a computed column.
         *
         * <p>The watermark generation expression is evaluated by the framework for every record
         * during runtime. The framework will periodically emit the largest generated watermark. If
         * the current watermark is still identical to the previous one, or is null, or the value of
         * the returned watermark is smaller than that of the last emitted one, then no new
         * watermark will be emitted. A watermark is emitted in an interval defined by the
         * configuration.
         *
         * <p>Any scalar expression can be used for declaring a watermark strategy for
         * in-memory/temporary tables. However, currently, only SQL expressions can be persisted in
         * a catalog. The expression's return data type must be {@code TIMESTAMP(3)}. User-defined
         * functions (also defined in different catalogs) are supported.
         *
         * <p>Example: {@code .watermark("ts", $("ts).minus(lit(5).seconds())}
         *
         * @param columnName the column name used as a rowtime attribute
         * @param watermarkExpression the expression used for watermark generation
         */
        public Builder watermark(String columnName, Expression watermarkExpression) {
            Preconditions.checkNotNull(columnName, "Column name must not be null.");
            Preconditions.checkNotNull(
                    watermarkExpression, "Watermark expression must not be null.");
            this.watermarkSpecs.add(new UnresolvedWatermarkSpec(columnName, watermarkExpression));
            return this;
        }

        /**
         * Declares that the given column should serve as an event-time (i.e. rowtime) attribute and
         * specifies a corresponding watermark strategy as an expression.
         *
         * <p>See {@link #watermark(String, Expression)} for a detailed explanation.
         *
         * <p>This method uses a SQL expression that can be easily persisted in a durable catalog.
         *
         * <p>Example: {@code .watermark("ts", "ts - INTERVAL '5' SECOND")}
         */
        public Builder watermark(String columnName, String sqlExpression) {
            return watermark(columnName, new SqlCallExpression(sqlExpression));
        }

        /**
         * Declares a primary key constraint for a list of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * <p>The primary key will be assigned a random name.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(String... columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKey(Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a list of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * <p>The primary key will be assigned a generated name in the format {@code PK_col1_col2}.
         *
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKey(List<String> columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            final String generatedConstraintName =
                    columnNames.stream().collect(Collectors.joining("_", "PK_", ""));
            return primaryKeyNamed(generatedConstraintName, columnNames);
        }

        /**
         * Declares a primary key constraint for a list of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * @param constraintName name for the primary key, can be used to reference the constraint
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKeyNamed(String constraintName, String... columnNames) {
            Preconditions.checkNotNull(columnNames, "Primary key column names must not be null.");
            return primaryKeyNamed(constraintName, Arrays.asList(columnNames));
        }

        /**
         * Declares a primary key constraint for a list of given columns. Primary key uniquely
         * identify a row in a table. Neither of columns in a primary can be nullable. The primary
         * key is informational only. It will not be enforced. It can be used for optimizations. It
         * is the data owner's responsibility to ensure uniqueness of the data.
         *
         * @param constraintName name for the primary key, can be used to reference the constraint
         * @param columnNames columns that form a unique primary key
         */
        public Builder primaryKeyNamed(String constraintName, List<String> columnNames) {
            Preconditions.checkState(
                    primaryKey == null, "Multiple primary keys are not supported.");
            Preconditions.checkNotNull(
                    constraintName, "Primary key constraint name must not be null.");
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(constraintName),
                    "Primary key constraint name must not be empty.");
            Preconditions.checkArgument(
                    columnNames != null && columnNames.size() > 0,
                    "Primary key constraint must be defined for at least a single column.");
            primaryKey = new UnresolvedPrimaryKey(constraintName, columnNames);
            return this;
        }

        /**
         * Declares a named index for a list of given column names. Indexes are designed to enable
         * very efficient search. The indexes are informational only and can be used for
         * optimizations. It is the data owner's responsibility to guarantee the index queries allow
         * the complete row to be retrieved efficiently.
         *
         * <p>The index will be assigned a generated name in the format {@code INDEX_col1_col2}.
         *
         * @param columnNames indexes that form a table index
         */
        public Builder index(String... columnNames) {
            Preconditions.checkNotNull(indexes, "Index column names must not be null.");
            return index(Arrays.asList(columnNames));
        }

        /**
         * Declares a named index for a list of given column names. Indexes are designed to enable
         * very efficient search. The indexes are informational only and can be used for
         * optimizations. It is the data owner's responsibility to guarantee the index queries allow
         * the complete row to be retrieved efficiently.
         *
         * <p>The index will be assigned a generated name in the format {@code INDEX_col1_col2}.
         *
         * @param columnNames indexes that form a table index
         */
        public Builder index(List<String> columnNames) {
            Preconditions.checkNotNull(indexes, "Index column names must not be null.");
            final String generatedIndexName =
                    columnNames.stream().collect(Collectors.joining("_", "INDEX_", ""));
            return indexNamed(generatedIndexName, columnNames);
        }

        /**
         * Declares a named index for a list of given column names. Indexes are designed to enable
         * very efficient search. The indexes are informational only and can be used for
         * optimizations. It is the data owner's responsibility to guarantee the index queries allow
         * the complete row to be retrieved efficiently.
         *
         * @param indexName the name of the index
         * @param columnNames columns that form a table index
         */
        public Builder indexNamed(String indexName, List<String> columnNames) {
            Preconditions.checkNotNull(indexName, "Index name must not be null.");
            Preconditions.checkNotNull(columnNames, "Index column names must not be null.");
            Preconditions.checkArgument(
                    !columnNames.isEmpty(), "Index must be defined for at least a single column.");
            this.indexes.add(new UnresolvedIndex(indexName, columnNames));
            return this;
        }

        /** Returns an instance of an unresolved {@link Schema}. */
        public Schema build() {
            return new Schema(columns, watermarkSpecs, primaryKey, indexes);
        }

        // ----------------------------------------------------------------------------------------

        private void addResolvedColumns(List<Column> columns) {
            for (Column c : columns) {
                if (c instanceof PhysicalColumn) {
                    PhysicalColumn physicalColumn = (PhysicalColumn) c;
                    column(physicalColumn.getName(), physicalColumn.getDataType());
                    c.getComment().ifPresent(this::withComment);
                } else if (c instanceof ComputedColumn) {
                    ComputedColumn computedColumn = (ComputedColumn) c;
                    columnByExpression(computedColumn.getName(), computedColumn.getExpression());
                    c.getComment().ifPresent(this::withComment);
                } else if (c instanceof MetadataColumn) {
                    MetadataColumn metadataColumn = (MetadataColumn) c;
                    columnByMetadata(
                            metadataColumn.getName(),
                            metadataColumn.getDataType(),
                            metadataColumn.getMetadataKey().orElse(null),
                            metadataColumn.isVirtual());
                    c.getComment().ifPresent(this::withComment);
                }
            }
        }

        private void addResolvedWatermarkSpec(List<WatermarkSpec> specs) {
            specs.forEach(
                    s ->
                            watermarkSpecs.add(
                                    new UnresolvedWatermarkSpec(
                                            s.getRowtimeAttribute(), s.getWatermarkExpression())));
        }

        private void addResolvedConstraint(UniqueConstraint constraint) {
            if (constraint.getType() == Constraint.ConstraintType.PRIMARY_KEY) {
                primaryKeyNamed(constraint.getName(), constraint.getColumns());
            } else {
                throw new IllegalArgumentException("Unsupported constraint type.");
            }
        }

        private void addResolvedIndexes(List<Index> resolvedIndexes) {
            resolvedIndexes.forEach(
                    index -> indexes.add(new UnresolvedIndex(index.getName(), index.getColumns())));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes for representing the schema
    // --------------------------------------------------------------------------------------------

    /** Super class for all kinds of columns in an unresolved schema. */
    @PublicEvolving
    public abstract static class UnresolvedColumn {
        final String columnName;
        final @Nullable String comment;

        UnresolvedColumn(String columnName, @Nullable String comment) {
            this.columnName = columnName;
            this.comment = comment;
        }

        public String getName() {
            return columnName;
        }

        public Optional<String> getComment() {
            return Optional.ofNullable(comment);
        }

        abstract UnresolvedColumn withComment(@Nullable String comment);

        @Override
        public String toString() {
            return EncodingUtils.escapeIdentifier(columnName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UnresolvedColumn that = (UnresolvedColumn) o;
            return columnName.equals(that.columnName) && Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName);
        }
    }

    /**
     * Declaration of a physical column that will be resolved to {@link PhysicalColumn} during
     * schema resolution.
     */
    @PublicEvolving
    public static final class UnresolvedPhysicalColumn extends UnresolvedColumn {

        private final AbstractDataType<?> dataType;

        public UnresolvedPhysicalColumn(String columnName, AbstractDataType<?> dataType) {
            this(columnName, dataType, null);
        }

        public UnresolvedPhysicalColumn(
                String columnName, AbstractDataType<?> dataType, @Nullable String comment) {
            super(columnName, comment);
            this.dataType = dataType;
        }

        @Override
        UnresolvedPhysicalColumn withComment(String comment) {
            return new UnresolvedPhysicalColumn(columnName, dataType, comment);
        }

        public AbstractDataType<?> getDataType() {
            return dataType;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s %s", super.toString(), dataType.toString()));
            getComment()
                    .ifPresent(
                            c -> {
                                sb.append(" COMMENT '");
                                sb.append(EncodingUtils.escapeSingleQuotes(c));
                                sb.append("'");
                            });
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            UnresolvedPhysicalColumn that = (UnresolvedPhysicalColumn) o;
            return dataType.equals(that.dataType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dataType);
        }
    }

    /**
     * Declaration of a computed column that will be resolved to {@link ComputedColumn} during
     * schema resolution.
     */
    @PublicEvolving
    public static final class UnresolvedComputedColumn extends UnresolvedColumn {

        private final Expression expression;

        public UnresolvedComputedColumn(String columnName, Expression expression) {
            this(columnName, expression, null);
        }

        public UnresolvedComputedColumn(String columnName, Expression expression, String comment) {
            super(columnName, comment);
            this.expression = expression;
        }

        @Override
        public UnresolvedComputedColumn withComment(String comment) {
            return new UnresolvedComputedColumn(columnName, expression, comment);
        }

        public Expression getExpression() {
            return expression;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s AS %s", super.toString(), expression.asSummaryString()));
            getComment()
                    .ifPresent(
                            c -> {
                                sb.append(" COMMENT '");
                                sb.append(EncodingUtils.escapeSingleQuotes(c));
                                sb.append("'");
                            });
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            UnresolvedComputedColumn that = (UnresolvedComputedColumn) o;
            return expression.equals(that.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), expression);
        }
    }

    /**
     * Declaration of a metadata column that will be resolved to {@link MetadataColumn} during
     * schema resolution.
     */
    @PublicEvolving
    public static final class UnresolvedMetadataColumn extends UnresolvedColumn {

        private final AbstractDataType<?> dataType;
        private final @Nullable String metadataKey;
        private final boolean isVirtual;

        public UnresolvedMetadataColumn(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual) {
            this(columnName, dataType, metadataKey, isVirtual, null);
        }

        public UnresolvedMetadataColumn(
                String columnName,
                AbstractDataType<?> dataType,
                @Nullable String metadataKey,
                boolean isVirtual,
                @Nullable String comment) {
            super(columnName, comment);
            this.dataType = dataType;
            this.metadataKey = metadataKey;
            this.isVirtual = isVirtual;
        }

        @Override
        UnresolvedMetadataColumn withComment(@Nullable String comment) {
            return new UnresolvedMetadataColumn(
                    columnName, dataType, metadataKey, isVirtual, comment);
        }

        public AbstractDataType<?> getDataType() {
            return dataType;
        }

        public @Nullable String getMetadataKey() {
            return metadataKey;
        }

        public boolean isVirtual() {
            return isVirtual;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(super.toString());
            sb.append(" METADATA");
            if (metadataKey != null) {
                sb.append(" FROM '");
                sb.append(EncodingUtils.escapeSingleQuotes(metadataKey));
                sb.append("'");
            }
            if (isVirtual) {
                sb.append(" VIRTUAL");
            }
            getComment()
                    .ifPresent(
                            c -> {
                                sb.append(" COMMENT '");
                                sb.append(EncodingUtils.escapeSingleQuotes(c));
                                sb.append("'");
                            });
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            UnresolvedMetadataColumn that = (UnresolvedMetadataColumn) o;
            return isVirtual == that.isVirtual
                    && dataType.equals(that.dataType)
                    && Objects.equals(metadataKey, that.metadataKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), dataType, metadataKey, isVirtual);
        }
    }

    /**
     * Declaration of a watermark strategy that will be resolved to {@link WatermarkSpec} during
     * schema resolution.
     */
    @PublicEvolving
    public static final class UnresolvedWatermarkSpec {

        private final String columnName;
        private final Expression watermarkExpression;

        public UnresolvedWatermarkSpec(String columnName, Expression watermarkExpression) {
            this.columnName = columnName;
            this.watermarkExpression = watermarkExpression;
        }

        public String getColumnName() {
            return columnName;
        }

        public Expression getWatermarkExpression() {
            return watermarkExpression;
        }

        @Override
        public String toString() {
            return String.format(
                    "WATERMARK FOR %s AS %s",
                    EncodingUtils.escapeIdentifier(columnName),
                    watermarkExpression.asSummaryString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UnresolvedWatermarkSpec that = (UnresolvedWatermarkSpec) o;
            return columnName.equals(that.columnName)
                    && watermarkExpression.equals(that.watermarkExpression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, watermarkExpression);
        }
    }

    /** Super class for all kinds of constraints in an unresolved schema. */
    @PublicEvolving
    public abstract static class UnresolvedConstraint {

        private final String constraintName;

        UnresolvedConstraint(String constraintName) {
            this.constraintName = constraintName;
        }

        public String getConstraintName() {
            return constraintName;
        }

        @Override
        public String toString() {
            return String.format("CONSTRAINT %s", EncodingUtils.escapeIdentifier(constraintName));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UnresolvedConstraint that = (UnresolvedConstraint) o;
            return constraintName.equals(that.constraintName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(constraintName);
        }
    }

    /**
     * Declaration of a primary key that will be resolved to {@link UniqueConstraint} during schema
     * resolution.
     */
    @PublicEvolving
    public static final class UnresolvedPrimaryKey extends UnresolvedConstraint {

        private final List<String> columnNames;

        public UnresolvedPrimaryKey(String constraintName, List<String> columnNames) {
            super(constraintName);
            this.columnNames = columnNames;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        @Override
        public String toString() {
            return String.format(
                    "%s PRIMARY KEY (%s) NOT ENFORCED",
                    super.toString(),
                    columnNames.stream()
                            .map(EncodingUtils::escapeIdentifier)
                            .collect(Collectors.joining(", ")));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            UnresolvedPrimaryKey that = (UnresolvedPrimaryKey) o;
            return columnNames.equals(that.columnNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), columnNames);
        }
    }

    /** Declaration of an index that will be resolved to {@link Index} during schema resolution. */
    @PublicEvolving
    public static final class UnresolvedIndex {
        private final String indexName;
        private final List<String> columnNames;

        public UnresolvedIndex(String indexName, List<String> columnNames) {
            this.indexName = indexName;
            this.columnNames = columnNames;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        @Override
        public String toString() {
            return String.format(
                    "INDEX %s (%s)",
                    EncodingUtils.escapeIdentifier(getIndexName()),
                    getColumnNames().stream()
                            .map(EncodingUtils::escapeIdentifier)
                            .collect(Collectors.joining(", ")));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UnresolvedIndex that = (UnresolvedIndex) o;
            return Objects.equals(indexName, that.indexName)
                    && Objects.equals(columnNames, that.columnNames);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexName, columnNames);
        }
    }
}
