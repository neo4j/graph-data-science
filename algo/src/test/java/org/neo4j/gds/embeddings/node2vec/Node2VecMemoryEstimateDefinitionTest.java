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
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.traversal.WalkParameters;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Node2VecMemoryEstimateDefinitionTest {

    @Test
    void shouldEstimateMemory() {
        var configMock = mock(Node2VecBaseConfig.class);
        when(configMock.embeddingDimension()).thenReturn(128);
        when(configMock.walkParameters()).thenReturn(new WalkParameters(10, 80, 1.0, 1.0));

        when(configMock.node2VecParameters()).thenCallRealMethod();

        var memoryEstimation = new Node2VecMemoryEstimateDefinition(configMock.node2VecParameters()).memoryEstimation();

        MemoryEstimationAssert.assertThat(memoryEstimation)
            .memoryRange(1000, new Concurrency(1))
            .hasSameMinAndMaxEqualTo(7688456L);
    }

}
