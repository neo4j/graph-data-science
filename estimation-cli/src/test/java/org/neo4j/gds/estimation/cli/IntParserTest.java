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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

class IntParserTest extends NumericParserTests<Integer> {

    @Override
    Integer run(String input) {
        return new IntParser().convert(input);
    }

    @Override
    Integer fromLong(long value) {
        int intValue = (int) value;
        assumeTrue(intValue == value, "Skipping the test against " + value + " as it cannot be represented as an integer.");
        return intValue;
    }
}
