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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.assertions.MemoryEstimationAssert;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Node2VecMemoryEstimateDefinitionTest {

    @Test
    void shouldEstimateMemory() {
        var configMock = mock(Node2VecBaseConfig.class);
        when(configMock.walksPerNode()).thenReturn(10);
        when(configMock.walkLength()).thenReturn(80);
        when(configMock.embeddingDimension()).thenReturn(128);

        var memoryEstimation = new Node2VecMemoryEstimateDefinition().memoryEstimation(configMock);

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(1000, 1)
            .hasSameMinAndMaxEqualTo(7_688_464L);
    }

}
