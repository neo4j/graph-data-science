/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import static org.junit.jupiter.api.Assertions.assertEquals;

class NumberParserTest extends NumericParserTests<Number>  {

    @Override
    Number run(String input) {
        return new NumberParser().convert(input);
    }

    @Override
    Number fromLong(long value) {
        var intValue = (int) value;
        if (intValue == value) {
            // explicitly box to an int, not a long, if it fits
            //noinspection UnnecessaryBoxing
            return Integer.valueOf(intValue);
        }
        return value;
    }

    @Test
    void parseInts() {
        assertEquals(42, run("42"));
    }

    @Test
    void parseLongs() {
        assertEquals(42_1337_42_1337L, run("421337421337"));
    }

    @Test
    void parseDoubles() {
        assertEquals(42.0D, run("42.0"));
        assertEquals(42.1337D, run("42.1337"));
        assertEquals(42.133742133742D, run("42.133742133742"));
    }
}
