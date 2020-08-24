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
package org.neo4j.gds.estimation.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class NumericParserTests<T extends Number> {

    abstract T run(String input);

    abstract T fromLong(long value);

    @ParameterizedTest
    @CsvSource({
        "42, 42",
        "1337, 1337",
        "421337421337, 421337421337"
    })
    void parseLongs(long expected, String input) {
        assertEquals(fromLong(expected), run(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"42k", "42K", "42 k", "42 K"})
    void parseKSuffix(String input) {
        assertEquals(fromLong(42_000L), run(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"42m", "42M", "42 m", "42 M"})
    void parseMSuffix(String input) {
        assertEquals(fromLong(42_000_000L), run(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"42g", "42G", "42 g", "42 G"})
    void parseGSuffix(String input) {
        assertEquals(fromLong(42_000_000_000L), run(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"42b", "42B", "42 b", "42 B"})
    void parseBSuffix(String input) {
        assertEquals(fromLong(42_000_000_000L), run(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"42t", "42T", "42 t", "42 T"})
    void parseTSuffix(String input) {
        assertEquals(fromLong(42_000_000_000_000L), run(input));
    }

    @Test
    void parseLongNumberWithSuffix() {
        assertEquals(fromLong(42_000_000L), run("42000K"));
    }

    @Test
    void parseLongNumberWithUnderscores() {
        assertEquals(fromLong(42_000L), run("42_000"));
        assertEquals(fromLong(42_1337_42L), run("42_1337_42"));
    }

    @Test
    void parseLongNumberWithUnderscoresAndSuffix() {
        assertEquals(fromLong(42_000_000L), run("42_000K"));
        assertEquals(fromLong(421_337_000L), run("42_13_37 k"));
    }

    @Test
    void parseLongsWithPrefix() {
        assertEquals(fromLong(42L), run("052"));
        assertEquals(fromLong(42L), run("0x2A"));
        assertEquals(fromLong(42L), run("0x2a"));
        assertEquals(fromLong(42L), run("#2a"));
    }
}
