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

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.api.DefaultValue.INTEGER_DEFAULT_FALLBACK;
import static org.neo4j.gds.api.DefaultValue.LONG_DEFAULT_FALLBACK;

class JacksonFileAppenderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldAppendDouble() throws IOException {
        var doublePropertySchema = PropertySchema.of("doublePropertySchema", ValueType.DOUBLE);
        var doublePropertyFilePath = tempDir.resolve("double-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            doublePropertyFilePath,
            List.of(doublePropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(1.9D);
            appender.endLine();
        }

        assertThat(doublePropertyFilePath)
            .isNotEmptyFile()
            .hasContent("1.9");
    }

    @Test
    void shouldAppendEmptyForDoubleFallbackValue() throws IOException {
        var doublePropertySchema = PropertySchema.of("doublePropertySchema", ValueType.DOUBLE);
        var doublePropertyFilePath = tempDir.resolve("double-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            doublePropertyFilePath,
            List.of(doublePropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(Double.NaN);
            appender.endLine();
        }

        assertThat(doublePropertyFilePath)
            .isNotEmptyFile()
            .hasContent(String.valueOf(CsvSchema.DEFAULT_LINEFEED));
    }

    @ParameterizedTest
    @ValueSource(longs = {
        LONG_DEFAULT_FALLBACK,
        INTEGER_DEFAULT_FALLBACK
    })
    void shouldAppendEmptyForLongFallbackValues(long fallbackValue) throws IOException {
        var longPropertySchema = PropertySchema.of("longPropertySchema", ValueType.LONG);
        var longPropertyFilePath = tempDir.resolve("long-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            longPropertyFilePath,
            List.of(longPropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(fallbackValue);
            appender.endLine();
        }

        assertThat(longPropertyFilePath)
            .isNotEmptyFile()
            .hasContent(String.valueOf(CsvSchema.DEFAULT_LINEFEED));
    }

    @Test
    void shouldAppendDoubleArray() throws IOException {
        var doubleArrayPropertySchema = PropertySchema.of("doubleArrayPropertySchema", ValueType.DOUBLE_ARRAY);
        var doubleArrayPropertyFilePath = tempDir.resolve("double-array-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            doubleArrayPropertyFilePath,
            List.of(doubleArrayPropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(new double[] {1.9D, 42D, 13.37D});
            appender.endLine();
        }

        assertThat(doubleArrayPropertyFilePath)
            .isNotEmptyFile()
            .hasContent("1.9;42.0;13.37");
    }

    @Test
    void shouldAppendEmptyForNullValue() throws IOException {
        var nullPropertySchema = PropertySchema.of("nullPropertySchema", ValueType.LONG);
        var nullPropertyFilePath = tempDir.resolve("null-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            nullPropertyFilePath,
            List.of(nullPropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.appendAny(null);
            appender.endLine();
        }

        assertThat(nullPropertyFilePath)
            .isNotEmptyFile()
            .hasContent(String.valueOf(CsvSchema.DEFAULT_LINEFEED));
    }

    @Test
    void shouldAppendLong() throws IOException {
        var longPropertySchema = PropertySchema.of("longPropertySchema", ValueType.LONG);
        var longPropertyFilePath = tempDir.resolve("long-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            longPropertyFilePath,
            List.of(longPropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(19L);
            appender.endLine();
        }

        assertThat(longPropertyFilePath)
            .isNotEmptyFile()
            .hasContent("19");
    }

    @Test
    void shouldAppendLongArray() throws IOException {
        var longArrayPropertySchema = PropertySchema.of("longArrayPropertySchema", ValueType.LONG_ARRAY);
        var longArrayPropertyFilePath = tempDir.resolve("long-array-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            longArrayPropertyFilePath,
            List.of(longArrayPropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(new long[] {19L, 42L, 1337L});
            appender.endLine();
        }

        assertThat(longArrayPropertyFilePath)
            .isNotEmptyFile()
            .hasContent("19;42;1337");
    }

    @Test
    void shouldAppendFloatArray() throws IOException {
        var floatArrayPropertySchema = PropertySchema.of("floatArrayPropertySchema", ValueType.FLOAT_ARRAY);
        var floatArrayPropertyFilePath = tempDir.resolve("float-array-property-test.csv");
        try (var appender = JacksonFileAppender.of(
            floatArrayPropertyFilePath,
            List.of(floatArrayPropertySchema),
            builder -> builder
        )) {
            appender.startLine();
            appender.append(new float[] {1.9F, 42F, 13.37F});
            appender.endLine();
        }

        assertThat(floatArrayPropertyFilePath)
            .isNotEmptyFile()
            .hasContent("1.9;42.0;13.37");
    }

    @Test
    void shouldAppendMultipleProperties() throws IOException {
        var doublePropertySchema = PropertySchema.of("doublePropertySchema", ValueType.DOUBLE);
        var nullPropertySchema = PropertySchema.of("earlyPropertySchema", ValueType.LONG);
        var longArrayPropertySchema = PropertySchema.of("longArrayPropertySchema", ValueType.LONG_ARRAY);
        var doubleArrayPropertySchema = PropertySchema.of("doubleArrayPropertySchema", ValueType.DOUBLE_ARRAY);

        var multiPropertyFilePath = tempDir.resolve("multi-property-test.csv");

        try (var appender = JacksonFileAppender.of(
            multiPropertyFilePath,
            List.of(doublePropertySchema, longArrayPropertySchema, doubleArrayPropertySchema, nullPropertySchema),
            builder -> builder.addNumberColumn(":ID")
        )) {
            appender.startLine();
            appender.appendAny(1337L);
            appender.appendAny(new double[] {1.9D, 42D, 13.37D});
            appender.appendAny(new double[] {1.95D});
            appender.appendAny(null);
            appender.appendAny(new long[] {19L, 42L, 1337L});
            appender.endLine();
        }

        assertThat(multiPropertyFilePath)
            .isNotEmptyFile()
            .hasContent("1337,1.9;42.0;13.37,1.95,,19;42;1337");

    }

}
