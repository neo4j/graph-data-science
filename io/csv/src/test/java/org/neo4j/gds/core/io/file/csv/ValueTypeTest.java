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
package org.neo4j.gds.core.io.file.csv;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.api.DefaultValue.DOUBLE_DEFAULT_FALLBACK;
import static org.neo4j.gds.api.DefaultValue.LONG_DEFAULT_FALLBACK;

class ValueTypeTest {

    private static Stream<Arguments> formatValues() {
        return Stream.of(
            arguments(ValueType.LONG, 42L, "42"),
            arguments(ValueType.LONG, LONG_DEFAULT_FALLBACK, ""),

            arguments(ValueType.DOUBLE, 42.1337D, "42.1337"),
            arguments(ValueType.DOUBLE, DOUBLE_DEFAULT_FALLBACK,  ""),

            arguments(ValueType.LONG_ARRAY, new long[]{1L, 3L, 3L, 7L}, "1;3;3;7"),
            arguments(ValueType.LONG_ARRAY, null, ""),

            arguments(ValueType.DOUBLE_ARRAY, new double[]{1.0D, 3.0D, 3.0D, 7.0D}, "1.0;3.0;3.0;7.0"),
            arguments(ValueType.DOUBLE_ARRAY, null, ""),

            arguments(ValueType.FLOAT_ARRAY, new float[]{1.0f, 3.0f, 3.0f, 7.0f}, "1.0;3.0;3.0;7.0"),
            arguments(ValueType.FLOAT_ARRAY, null, "")
        );
    }

    @ParameterizedTest
    @MethodSource("formatValues")
    void testParsingFromCsv(ValueType valueType, Object expected, String value) throws JsonProcessingException {
        var schema = CsvSchema.builder().addColumn("a", CsvSchemaUtil.csvTypeFromValueType(valueType)).build();
        var tree = new CsvMapper().reader().with(schema).readTree(value);
        var node = tree.get("a");
        assertThat(valueType.fromCsvValue(node)).isEqualTo(expected);
    }
}
