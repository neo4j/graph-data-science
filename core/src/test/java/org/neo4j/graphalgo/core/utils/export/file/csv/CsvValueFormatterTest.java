/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export.file.csv;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.api.DefaultValue.DOUBLE_DEFAULT_FALLBACK;
import static org.neo4j.graphalgo.api.DefaultValue.INTEGER_DEFAULT_FALLBACK;
import static org.neo4j.graphalgo.api.DefaultValue.LONG_DEFAULT_FALLBACK;
import static org.neo4j.graphalgo.core.utils.export.file.csv.CsvValueFormatter.format;

class CsvValueFormatterTest {


    private static Stream<Arguments> formatValues() {
        return Stream.of(
            arguments(42L, "42"),
            arguments(LONG_DEFAULT_FALLBACK, ""),

            arguments(42, "42"),
            arguments(INTEGER_DEFAULT_FALLBACK, ""),

            arguments(42.1337D, "42.1337"),
            arguments(DOUBLE_DEFAULT_FALLBACK,  ""),

            arguments(42.1337F, "42.1337"),
            arguments(DOUBLE_DEFAULT_FALLBACK, ""),

            arguments(new long[]{1L, 3L, 3L, 7L}, "1;3;3;7"),
            arguments(null, ""),

            arguments(new double[]{1.0D, 3.0D, 3.0D, 7.0D}, "1.0;3.0;3.0;7.0"),
            arguments(null, ""),

            arguments(new double[]{1.0f, 3.0f, 3.0f, 7.0f}, "1.0;3.0;3.0;7.0"),
            arguments(null, "")
        );
    }

    @ParameterizedTest
    @MethodSource("formatValues")
    void testFormatting(Object value, String expected) {
        assertThat(format(value)).isEqualTo(expected);
    }

}
