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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.junit.annotation.Edition;
import org.neo4j.graphalgo.junit.annotation.GdsEditionTest;
import org.neo4j.graphalgo.utils.CheckedRunnable;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NativeFactoryTest {

    @Test
    @GdsEditionTest(Edition.EE)
    void memoryEstimationBitMapEnabled() {
        var expectedMinUsage = 1818450752L;
        var expectedMaxUsage = 3018637544L;
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .maxRelCount(500_000_000L)
            .build();

        var memoryEstimation = new AtomicReference<MemoryEstimation>();
        var runnable = CheckedRunnable.runnable(() ->
            memoryEstimation.set(NativeFactory.getMemoryEstimation(
                NodeProjections.all(),
                RelationshipProjections.single(RelationshipType.ALL_RELATIONSHIPS, RelationshipProjection.ALL)
            )));

        GdsFeatureToggles.USE_BIT_ID_MAP.enableAndRun(runnable);

        var estimate = memoryEstimation.get().estimate(dimensions, 1);
        assertEquals(expectedMinUsage, estimate.memoryUsage().min);
        assertEquals(expectedMaxUsage, estimate.memoryUsage().max);
    }

    @Test
    void memoryEstimationBitMapDisabled() {
        var expectedMinUsage = 3405981464L;
        var expectedMaxUsage = 4606168256L;
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .maxRelCount(500_000_000L)
            .build();

        var memoryEstimation = new AtomicReference<MemoryEstimation>();
        memoryEstimation.set(NativeFactory.getMemoryEstimation(
            NodeProjections.all(),
            RelationshipProjections.single(RelationshipType.ALL_RELATIONSHIPS, RelationshipProjection.ALL)
        ));


        var estimate = memoryEstimation.get().estimate(dimensions, 1);
        assertEquals(expectedMinUsage, estimate.memoryUsage().min);
        assertEquals(expectedMaxUsage, estimate.memoryUsage().max);
    }

    @Test
    void memoryEstimationForMultipleProjections() {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(100_000_000L)
            .putRelationshipCount(RelationshipType.of("TYPE1"), 250_000_000L)
            .putRelationshipCount(RelationshipType.of("TYPE2"), 250_000_000L)
            .build();

        NodeProjections nodeProjections = NodeProjections.all();
        RelationshipProjections relationshipProjections = RelationshipProjections
            .builder()
            .putProjection(RelationshipType.of("TYPE1"), RelationshipProjection.of("TYPE1", Orientation.NATURAL))
            .putProjection(RelationshipType.of("TYPE2"), RelationshipProjection.of("TYPE2", Orientation.NATURAL))
            .build();

        MemoryTree estimate = NativeFactory.getMemoryEstimation(nodeProjections, relationshipProjections).estimate(dimensions, 1);
        long idMapMemoryUsage = IdMap.memoryEstimation().estimate(dimensions, 1).memoryUsage().min;
        int instanceSize = 72;

        assertEquals(3_205_950_324L * 2 - idMapMemoryUsage - instanceSize, estimate.memoryUsage().min);
        assertEquals(6_011_568_224L, estimate.memoryUsage().max);
    }
}
