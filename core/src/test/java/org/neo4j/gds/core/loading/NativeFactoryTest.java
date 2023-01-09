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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NativeFactoryTest {

    @Test
    void memoryEstimationBitMapDisabled() {
        var expectedMinUsage = 3400612752L;
        var expectedMaxUsage = 4600799544L;
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .relCountUpperBound(500_000_000L)
            .build();

        var memoryEstimation = new AtomicReference<MemoryEstimation>();
        memoryEstimation.set(NativeFactory.getMemoryEstimation(
            NodeProjections.all(),
            RelationshipProjections.single(RelationshipType.ALL_RELATIONSHIPS, RelationshipProjection.ALL),
            false
        ));


        var estimate = memoryEstimation.get().estimate(dimensions, 1);
        assertEquals(expectedMinUsage, estimate.memoryUsage().min);
        assertEquals(expectedMaxUsage, estimate.memoryUsage().max);
    }

    @Test
    void memoryEstimationForSingleProjection() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .putRelationshipCount(RelationshipType.of("TYPE1"), 250_000_000L)
            .build();

        NodeProjections nodeProjections = NodeProjections.all();
        RelationshipProjections relationshipProjections = ImmutableRelationshipProjections
            .builder()
            .putProjection(
                RelationshipType.of("TYPE1"),
                RelationshipProjection
                    .builder()
                    .type("TYPE1")
                    .orientation(Orientation.NATURAL)
                    .build()
            )
            .build();

        MemoryTree estimate = NativeFactory
            .getMemoryEstimation(nodeProjections, relationshipProjections, true)
            .estimate(dimensions, 1);

        assertEquals(6_828_526_776L, estimate.memoryUsage().min);
        assertEquals(7_633_833_144L, estimate.memoryUsage().max);
    }

    @Test
    void memoryEstimationForIndexedProjection() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .putRelationshipCount(RelationshipType.of("TYPE1"), 250_000_000L)
            .build();

        NodeProjections nodeProjections = NodeProjections.all();
        RelationshipProjections relationshipProjections = ImmutableRelationshipProjections
            .builder()
            .putProjection(
                RelationshipType.of("TYPE1"),
                RelationshipProjection
                    .builder()
                    .type("TYPE1")
                    .orientation(Orientation.NATURAL)
                    .indexInverse(true)
                    .build()
            )
            .build();

        MemoryTree estimate = NativeFactory
            .getMemoryEstimation(nodeProjections, relationshipProjections, true)
            .estimate(dimensions, 1);

        assertEquals(12_056_534_400L, estimate.memoryUsage().min);
        assertEquals(13_667_147_136L, estimate.memoryUsage().max);
    }

    @Test
    void memoryEstimationForMultipleProjections() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .putRelationshipCount(RelationshipType.of("TYPE1"), 250_000_000L)
            .putRelationshipCount(RelationshipType.of("TYPE2"), 250_000_000L)
            .build();

        NodeProjections nodeProjections = NodeProjections.all();
        RelationshipProjections relationshipProjections = ImmutableRelationshipProjections
            .builder()
            .putProjection(RelationshipType.of("TYPE1"), RelationshipProjection.of("TYPE1", Orientation.NATURAL))
            .putProjection(RelationshipType.of("TYPE2"), RelationshipProjection.of("TYPE2", Orientation.NATURAL))
            .build();

        MemoryTree estimate = NativeFactory
            .getMemoryEstimation(nodeProjections, relationshipProjections, true)
            .estimate(dimensions, 1);

        assertEquals(12_056_534_400L, estimate.memoryUsage().min);
        assertEquals(13_667_147_136L, estimate.memoryUsage().max);
    }
}
