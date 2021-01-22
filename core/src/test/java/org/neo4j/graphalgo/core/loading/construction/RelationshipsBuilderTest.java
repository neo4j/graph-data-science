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
package org.neo4j.graphalgo.core.loading.construction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.crossArguments;

class RelationshipsBuilderTest {

    static Stream<Arguments> propertiesAndIdMaps() {
        return crossArguments(
            () -> Stream.of(Arguments.of(true), Arguments.of(false)),
            () -> TestMethodRunner.idMapImplementation().map(Arguments::of)
        );
    }

    @ParameterizedTest()
    @MethodSource("propertiesAndIdMaps")
    void parallelRelationshipImport(boolean importProperty, TestMethodRunner runTest) {
        var concurrency = 4;
        var nodeCount = 100;
        var relationshipCount = 1000;

        var idMap = createIdMap(nodeCount, runTest);

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(Orientation.NATURAL)
            .loadRelationshipProperty(importProperty)
            .concurrency(concurrency)
            .build();

        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, relationshipCount),
            concurrency,
            stream -> stream.forEach(relId -> {
                if (importProperty) {
                    relationshipsBuilder.addFromInternal(relId % nodeCount, relId % nodeCount + 1, relId);
                } else {
                    relationshipsBuilder.addFromInternal(relId % nodeCount, relId % nodeCount + 1);
                }
            })
        );

        var relationships = relationshipsBuilder.build();
        assertEquals(relationshipCount, relationships.topology().elementCount());
        assertEquals(Orientation.NATURAL, relationships.topology().orientation());

        var graph = GraphFactory.create(idMap, relationships, AllocationTracker.empty());

        graph.forEachNode(nodeId -> {
            assertEquals(10, graph.degree(nodeId));

            graph.forEachRelationship(nodeId, Double.NaN, ((sourceNodeId, targetNodeId, weight) -> {
                assertEquals(sourceNodeId, targetNodeId - 1, "Incorrect source, target combination");
                if (importProperty) {
                    assertEquals(weight % nodeCount, sourceNodeId, "Incorrect weight");
                }
                return true;
            }));

            return true;
        });
    }


    private NodeMapping createIdMap(long nodeCount, TestMethodRunner runTest) {
        var nodesBuilderRef = new AtomicReference<NodeMapping>();
        runTest.run(() -> {
            var nodesBuilder = GraphFactory.initNodesBuilder().maxOriginalId(nodeCount).build();

            for (long i = 0; i < nodeCount; i++) {
                nodesBuilder.addNode(i);
            }

            nodesBuilderRef.set(nodesBuilder.build());
        });

        return nodesBuilderRef.get();
    }

}
