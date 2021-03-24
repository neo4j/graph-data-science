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
package org.neo4j.graphalgo.results;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.scaling.Scaler;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.result.CentralityResult;

import static org.assertj.core.api.Assertions.assertThat;

class NormalizedCentralityResultTest {
    @Test
    void mapsThroughScaler() {
        var unNormalizedResult = new CentralityResult(HugeDoubleArray.of(1, 2, 3, 4));
        var normalizedResult = new NormalizedCentralityResult(unNormalizedResult, Scaler.ZERO_SCALER);

        var normalizedProperties = normalizedResult.asNodeProperties();
        for (int i = 0; i < 4; i++) {
            assertThat(normalizedResult.score(i)).isEqualTo(0D);
            assertThat(normalizedProperties.doubleValue(i)).isEqualTo(0D);
        }
    }
}
