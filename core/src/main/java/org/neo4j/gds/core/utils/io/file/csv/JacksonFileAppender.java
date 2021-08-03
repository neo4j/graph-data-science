/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.core.utils.io.file.csv;

import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.schema.PropertySchema;

import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.neo4j.gds.api.DefaultValue.INTEGER_DEFAULT_FALLBACK;
import static org.neo4j.gds.api.DefaultValue.LONG_DEFAULT_FALLBACK;

final class JacksonFileAppender implements Flushable, AutoCloseable {

    private final CsvGenerator csvEncoder;
    private final CsvSchema csvSchema;

    private int currentColumnIndex = 0;

    static <PROPERTY_SCHEMA extends PropertySchema> JacksonFileAppender of(
        Path filePath,
        List<PROPERTY_SCHEMA> propertySchemas,
        UnaryOperator<CsvSchema.Builder> schemaEnricher
    ) {
        var csvSchemaBuilder = schemaEnricher.apply(CsvSchema.builder());
        for (PROPERTY_SCHEMA propertySchema : propertySchemas) {
            switch (propertySchema.valueType()) {
                case LONG:
                case DOUBLE:
                    csvSchemaBuilder.addNumberColumn(propertySchema.key());
                    break;
                case DOUBLE_ARRAY:
                case FLOAT_ARRAY:
                case LONG_ARRAY:
                    csvSchemaBuilder.addArrayColumn(propertySchema.key(), ";");
                    break;
                case STRING:
                    csvSchemaBuilder.addColumn(propertySchema.key());
                    break;
                case UNKNOWN:
                    break;
            }
        }
        var csvSchema = csvSchemaBuilder.build();

        var mapper = CsvMapper.csvBuilder().build();
        var factory = mapper.getFactory();

        try {
            var writer = Files.newBufferedWriter(filePath, StandardCharsets.UTF_8);
            var csvEncoder = factory.createGenerator(writer);
            csvEncoder.setSchema(csvSchema);
            return new JacksonFileAppender(csvEncoder, csvSchema);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private JacksonFileAppender(
        CsvGenerator csvEncoder,
        CsvSchema csvSchema
    ) {
        this.csvEncoder = csvEncoder;
        this.csvSchema = csvSchema;
    }

    void append(long value) throws IOException {
        if (value != LONG_DEFAULT_FALLBACK && value != INTEGER_DEFAULT_FALLBACK) {
            setFieldName();
            csvEncoder.writeNumber(value);
        } else {
            appendEmptyField();
        }

    }

    void append(double value) throws IOException {
        if (!Double.isNaN(value)) {
            setFieldName();
            csvEncoder.writeNumber(value);
        } else {
            appendEmptyField();
        }
    }

    void append(String value) throws IOException {
        setFieldName();
        csvEncoder.writeString(value);
    }

    void append(double[] value) throws IOException {
        setFieldName();
        csvEncoder.writeArray(value, 0, value.length);
    }

    void append(long[] value) throws IOException {
        setFieldName();
        csvEncoder.writeArray(value, 0, value.length);
    }

    void append(float[] value) throws IOException {
        setFieldName();
        csvEncoder.writeStartArray(value, value.length);
        for (float v : value) {
            csvEncoder.writeNumber(v);
        }
        csvEncoder.writeEndArray();
    }

    void appendAny(@Nullable Object value) throws IOException {
        if (value instanceof Double) {
            append((double) value);
        } else if (value instanceof Long) {
            append((long) value);
        } else if (value instanceof double[]) {
            append((double[]) value);
        } else if (value instanceof long[]) {
            append((long[]) value);
        } else if (value instanceof float[]) {
            append((float[]) value);
        } else if (value instanceof String) {
            append((String) value);
        } else if (value == null) {
            appendEmptyField();
        }
    }

    void startLine() throws IOException {
        currentColumnIndex = 0;
        csvEncoder.writeStartObject();
    }

    void endLine() throws IOException {
        csvEncoder.writeEndObject();
    }

    @Override
    public void flush() throws IOException {
        csvEncoder.flush();
    }

    @Override
    public void close() throws IOException {
        csvEncoder.close();
    }

    private void appendEmptyField() throws IOException {
        setFieldName();
        csvEncoder.writeNull();
    }

    private void setFieldName() throws IOException {
        var column = csvSchema.column(currentColumnIndex++);
        csvEncoder.writeFieldName(column.getName());
    }
}
