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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.values.storable.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class NodesBuilderTest {

    @GdlGraph
    static final String DB_CYPHER = "CREATE" +
                                    "  (a:A {prop1: 42, prop2: 1337})" +
                                    ", (b:A {prop1: 43, prop2: 1338})";

    @Inject
    Graph graph;

    @Inject
    IdFunction idFunction;

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void parallelIdMapBuilder(TestMethodRunner runTest) {
        runTest.run(() -> {
            long nodeCount = 100;
            int concurrency = 4;
            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(nodeCount)
                .concurrency(concurrency)
                .tracker(AllocationTracker.empty())
                .build();

            ParallelUtil.parallelStreamConsume(
                LongStream.range(0, nodeCount),
                concurrency,
                stream -> stream.forEach(nodesBuilder::addNode)
            );

            var idMap = nodesBuilder.build().nodeMapping();

            assertEquals(nodeCount, idMap.nodeCount());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void parallelIdMapBuilderWithDuplicateNodes(TestMethodRunner runTest) {
        runTest.run(() -> {
            long attempts = 100;
            int concurrency = 4;
            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(attempts)
                .concurrency(concurrency)
                .tracker(AllocationTracker.empty())
                .build();

            ParallelUtil.parallelStreamConsume(
                LongStream.range(0, attempts),
                concurrency,
                stream -> stream.forEach(originalId -> nodesBuilder.addNode(0))
            );

            var idMap = nodesBuilder.build().nodeMapping();

            assertEquals(1, idMap.nodeCount());
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void parallelIdMapBuilderWithLabels(TestMethodRunner runTest) {
        runTest.run(() -> {
            long attempts = 100;
            int concurrency = 1;
            var labels1 = new HashSet<>(NodeLabel.listOf("Label1"));
            var labels2 = new HashSet<>(NodeLabel.listOf("Label2"));

            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(attempts)
                .hasLabelInformation(true)
                .concurrency(concurrency)
                .tracker(AllocationTracker.empty())
                .build();

            ParallelUtil.parallelStreamConsume(LongStream.range(0, attempts), concurrency, stream -> stream.forEach(
                originalId -> {
                    var labels = originalId % 2 == 0
                        ? labels1.toArray(NodeLabel[]::new)
                        : labels2.toArray(NodeLabel[]::new);

                    nodesBuilder.addNode(originalId, labels);
                })
            );

            var idMap = nodesBuilder.build().nodeMapping();

            var expectedLabels = new HashSet<NodeLabel>();
            expectedLabels.addAll(labels1);
            expectedLabels.addAll(labels2);
            assertEquals(expectedLabels, idMap.availableNodeLabels());

            idMap.forEachNode(nodeId -> {
                var labels = idMap.toOriginalNodeId(nodeId) % 2 == 0
                    ? labels1
                    : labels2;

                assertEquals(labels, idMap.nodeLabels(nodeId));

                return true;
            });
        });
    }


    @Test
    void shouldBuildNodesWithProperties() {
        NodesBuilder nodesBuilder = NodesBuilder.fromSchema(
            2,
            graph.nodeCount(),
            1,
            graph.schema().nodeSchema(),
            AllocationTracker.empty()
        );

        nodesBuilder.addNode(idFunction.of("a"), Map.of("prop1", Values.longValue(42), "prop2", Values.longValue(1337)), NodeLabel.of("A"));
        nodesBuilder.addNode(idFunction.of("b"), Map.of("prop1", Values.longValue(43), "prop2", Values.longValue(1338)), NodeLabel.of("A"));

        var nodeMappingAndProperties = nodesBuilder.build();
        var nodeMapping = nodeMappingAndProperties.nodeMapping();
        var nodeProperties = nodeMappingAndProperties
            .nodeProperties()
            .orElseThrow(() -> new IllegalArgumentException("Expected node properties to be present"));

        assertThat(graph.nodeCount()).isEqualTo(nodeMapping.nodeCount());
        assertThat(graph.availableNodeLabels()).isEqualTo(nodeMapping.availableNodeLabels());
        graph.forEachNode(nodeId -> {
            assertThat(nodeMapping.toOriginalNodeId(nodeId)).isEqualTo(graph.toOriginalNodeId(nodeId));
            assertThat(nodeProperties.get("prop1").longValue(nodeId)).isEqualTo(graph.nodeProperties("prop1").longValue(nodeId));
            assertThat(nodeProperties.get("prop2").longValue(nodeId)).isEqualTo(graph.nodeProperties("prop2").longValue(nodeId));
            return true;
        });
    }

    @Test
    void shouldBuildNodesWithPropertiesInParallel() {
        int nodeCount = 10000;
        var nodeLabel = NodeLabel.of("A");
        var nodeSchema = NodeSchema.builder()
            .addProperty(nodeLabel, "prop1", ValueType.LONG)
            .addProperty(nodeLabel, "prop2", ValueType.LONG)
            .build();
        int concurrency = 4;
        var nodesBuilder = NodesBuilder.fromSchema(
            nodeCount,
            nodeCount,
            concurrency,
            nodeSchema,
            AllocationTracker.empty()
        );

        var tasks = ParallelUtil.tasks(
            concurrency,
            (index) -> () -> IntStream.range(index * (nodeCount / concurrency), (index + 1) * (nodeCount / concurrency))
                .forEach(originalNodeId -> nodesBuilder.addNode(
                    originalNodeId,
                    Map.of("prop1", Values.longValue(originalNodeId), "prop2", Values.longValue(nodeCount - originalNodeId)),
                    nodeLabel
                ))
        );

        ParallelUtil.run(tasks, Pools.DEFAULT);

        var nodeMappingAndProperties = nodesBuilder.build();
        var nodeMapping = nodeMappingAndProperties.nodeMapping();
        var nodeProperties = nodeMappingAndProperties.nodePropertiesOrThrow();

        assertThat(nodeMapping.nodeCount()).isEqualTo(nodeCount);
        assertThat(nodeMapping.availableNodeLabels()).containsExactly(nodeLabel);

        nodeMapping.forEachNode(nodeId -> {
            long originalNodeId = nodeMapping.toOriginalNodeId(nodeId);
            assertThat(originalNodeId).isEqualTo(nodeProperties.get("prop1").longValue(nodeId));
            assertThat(nodeCount - originalNodeId).isEqualTo(nodeProperties.get("prop2").longValue(nodeId));
            return true;
        });
    }
}
