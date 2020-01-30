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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.RelationshipProjectionMappings;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import static org.junit.jupiter.api.Assertions.*;

class HugeGraphFactoryTest {

    @Test
    void memoryEstimation() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .maxRelCount(500_000_000L)
            .relationshipProjectionMappings(RelationshipProjectionMappings.all())
            .build();

        MemoryEstimation memoryEstimation = HugeGraphFactory.getMemoryEstimation(dimensions);
        MemoryTree estimate = memoryEstimation.estimate(dimensions, 1);
        assertEquals(3_405_981_448L, estimate.memoryUsage().min);
        assertEquals(4_606_168_240L, estimate.memoryUsage().max);
    }

    @Test
    void memoryEstimationForMultipleProjections() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .relationshipProjectionMappings(RelationshipProjectionMappings.of(
                RelationshipProjectionMapping.of("TYPE1", "TYPE1", Projection.NATURAL, -1),
                RelationshipProjectionMapping.of("TYPE2", "TYPE2", Projection.REVERSE, -1)
            ))
            .putRelationshipCount("TYPE1", 250_000_000L)
            .putRelationshipCount("TYPE2", 250_000_000L)
            .build();

        MemoryTree estimate = HugeGraphFactory.getMemoryEstimation(dimensions).estimate(dimensions, 1);
        long idMapMemoryUsage = IdMap.memoryEstimation().estimate(dimensions, 1).memoryUsage().min;
        int instanceSize = 72;

        assertEquals(3_205_950_312L * 2 - idMapMemoryUsage - instanceSize, estimate.memoryUsage().min);
        assertEquals(6_011_568_216L, estimate.memoryUsage().max);
    }

}