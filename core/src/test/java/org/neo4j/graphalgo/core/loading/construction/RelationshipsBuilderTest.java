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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.huge.NodeFilteredGraph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.gdl.GdlFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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

    @ParameterizedTest
    @ValueSource(ints = {1, 10000})
    void testOnFilteredIdMap(int numberOfBNodes) {
        var graphString = graphCreateString(numberOfBNodes, "");

        var gdlFactory = GdlFactory.of(graphString);
        var graphStore = gdlFactory.build().graphStore();
        var idFunction = (IdFunction) gdlFactory::nodeId;

        var graph = graphStore.getGraph("A", "REL", Optional.empty());

        assertThat(graph).isInstanceOf(NodeFilteredGraph.class);
        assertThat(graph.nodeCount()).isEqualTo(2L);

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(graph)
            .build();

        var a1Original = idFunction.of("a1");
        var a2Original = idFunction.of("a2");

        var a1MappedId = graph.toMappedNodeId(a1Original);
        var a2MappedId = graph.toMappedNodeId(a2Original);

        assertThat(a1MappedId).isEqualTo(0);
        assertThat(a2MappedId).isEqualTo(1);

        var unionGraph = graphStore.getUnion();

        assertThat(unionGraph.toMappedNodeId(a1Original)).isNotEqualTo(a1MappedId);
        assertThat(unionGraph.toMappedNodeId(a2Original)).isNotEqualTo(a2MappedId);
        relationshipsBuilder.addFromInternal(a1MappedId, a2MappedId);
        var relationships = relationshipsBuilder.build();

        graphStore.addRelationshipType(
            RelationshipType.of("TEST"),
            Optional.empty(),
            Optional.empty(),
            relationships
        );

        var expectedGraphString = graphCreateString(
            numberOfBNodes,
            ", (a1)-[:TEST]->(a2)"
        );

        assertGraphEquals(
            TestSupport.fromGdl(expectedGraphString),
            graphStore.getUnion()
        );
    }

    @NotNull
    private String graphCreateString(int numberOfBNodes, String additional) {
        return IntStream.range(0, numberOfBNodes)
            .mapToObj(ignore -> "(:B)")
            .collect(Collectors.joining(
                ", ",
                "CREATE ",
                formatWithLocale(", (a1: A), (a2: A), (a1)-[:REL]->(a2)%s", additional)
            ));
    }
}
