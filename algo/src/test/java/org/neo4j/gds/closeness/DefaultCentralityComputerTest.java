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
package org.neo4j.gds.closeness;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultCentralityComputerTest {

    @ParameterizedTest
    @CsvSource({
        "5, 5, 1.0",
        "10, 5, 0.5",
        "0, 0, 0",
    })
    void centrality(long farness, long componentSize, double expectedScore) {

        var centralityComputer = new DefaultCentralityComputer();

        assertThat(centralityComputer.centrality(farness, componentSize))
            .isEqualTo(expectedScore, Offset.offset(0.01));
    }
}
