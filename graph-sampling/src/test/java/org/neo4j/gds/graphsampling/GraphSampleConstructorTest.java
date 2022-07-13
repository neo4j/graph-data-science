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
package org.neo4j.gds.graphsampling;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.core.loading.ArrayIdMap;
import org.neo4j.gds.core.loading.LabelInformation;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfigImpl;
import org.neo4j.gds.graphsampling.samplers.NodesSampler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@GdlExtension
class GraphSampleConstructorTest {

    @GdlGraph(idOffset = 42)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (x:Z {prop: 42})" +
        ", (x1:Z {prop: 43})" +
        ", (x2:Z {prop: 44})" +
        ", (x3:Z {prop: 45})" +
        ", (a:N {prop: 46})" +
        ", (b:N {prop: 47})" +
        ", (c:N {prop: 48, attr: 48})" +
        ", (d:N {prop: 49, attr: 48})" +
        ", (e:M {prop: 50, attr: 48})" +
        ", (f:M {prop: 51, attr: 48})" +
        ", (g:M {prop: 52})" +
        ", (h:M {prop: 53})" +
        ", (i:X {prop: 54})" +
        ", (j:M {prop: 55})" +
        ", (x)-[:R1]->(x1)" +
        ", (x)-[:R1]->(x2)" +
        ", (x)-[:R1]->(x3)" +
        ", (e)-[:R1]->(d)" +
        ", (i)-[:R1]->(g)" +
        ", (a)-[:R1 {cost: 10.0, distance: 5.8}]->(b)" +
        ", (a)-[:R1 {cost: 10.0, distance: 4.8}]->(c)" +
        ", (c)-[:R1 {cost: 10.0, distance: 5.8}]->(d)" +
        ", (d)-[:R1 {cost:  4.2, distance: 2.6}]->(e)" +
        ", (e)-[:R1 {cost: 10.0, distance: 5.8}]->(f)" +
        ", (f)-[:R1 {cost: 10.0, distance: 9.9}]->(g)" +
        ", (h)-[:R2 {cost: 10.0, distance: 5.8}]->(i)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    class TestNodesSampler implements NodesSampler {

        private final IdMap idMap;

        TestNodesSampler(List<Long> originalIds, Map<NodeLabel, List<Long>> nodeLabels) {
            long nodeCount = originalIds.size();
            var mappedToOriginal = HugeLongArray.newArray(nodeCount);
            mappedToOriginal.setAll(nodeId -> originalIds.get((int) nodeId));

            var originalToMappedBuilder = HugeSparseLongArray.builder(IdMap.NOT_FOUND, nodeCount);
            for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
                originalToMappedBuilder.set(originalIds.get(nodeId), nodeId);
            }
            var originalToMapped = originalToMappedBuilder.build();

            var labelInfoBuilder = LabelInformation.builder(10);
            nodeLabels.forEach((label, nodes) -> {
                nodes.forEach(originalId -> labelInfoBuilder.addNodeIdToLabel(label, originalId));
            });

            this.idMap = new ArrayIdMap(
                mappedToOriginal,
                originalToMapped,
                labelInfoBuilder.build(nodeCount, originalToMapped::get),
                nodeCount,
                originalIds.stream().mapToLong(Long::longValue).max().getAsLong()
            );
        }

        @Override
        public IdMap sampleNodes(Graph inputGraph) {
            return idMap;
        }
    }

    Map<NodeLabel, List<Long>> nobeLabels(List<Long> originalIds) {
        var nodeLabels = new HashMap<NodeLabel, List<Long>>();

        for (var label : graphStore.nodeLabels()) {
            var graph = graphStore.getGraph(label);
            var nodeIds = new ArrayList<Long>();

            graph.forEachNode(nodeId -> nodeIds.add(graph.toOriginalNodeId(nodeId)));
            var validNodeIds = nodeIds.stream().filter(originalIds::contains).collect(Collectors.toList());

            if (!validNodeIds.isEmpty()) {
                nodeLabels.put(label, validNodeIds);
            }
        }

        return nodeLabels;
    }

    @Test
    void shouldSampleAndFilterSchema() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNode(idFunction.of("a"))
            .samplingRatio(0.5)
            .restartProbability(0.1)
            .randomSeed(42L)
            .build();

        var originalIds = List.of(
            idFunction.of("a"),
            idFunction.of("b"),
            idFunction.of("c"),
            idFunction.of("d"),
            idFunction.of("e"),
            idFunction.of("f"),
            idFunction.of("g")
        );

        var rwrGraphConstructor = new GraphSampleConstructor(
            config,
            graphStore,
            new TestNodesSampler(originalIds, nobeLabels(originalIds))
        );

        var subgraph = rwrGraphConstructor.construct();
        assertThat(subgraph.getUnion().nodeCount()).isEqualTo(7);
        assertThat(graphStore.schema().nodeSchema().filter(Set.of(NodeLabel.of("N"), NodeLabel.of("M"))))
            .usingRecursiveComparison()
            .isEqualTo(subgraph.schema().nodeSchema());
        assertThat(graphStore.schema().relationshipSchema().filter(Set.of(RelationshipType.of("R1"))))
            .usingRecursiveComparison()
            .isEqualTo(subgraph.schema().relationshipSchema());
        assertThat(graphStore.capabilities()).usingRecursiveComparison().isEqualTo(subgraph.capabilities());
        assertThat(graphStore.databaseId()).usingRecursiveComparison().isEqualTo(subgraph.databaseId());

        var expectedGraph =
            "  (a:N {prop: 46})" +
            ", (b:N {prop: 47})" +
            ", (c:N {prop: 48, attr: 48})" +
            ", (d:N {prop: 49, attr: 48})" +
            ", (e:M {prop: 50, attr: 48})" +
            ", (f:M {prop: 51, attr: 48})" +
            ", (g:M {prop: 52})" +
            ", (e)-[:R1]->(d)" +
            ", (a)-[:R1 {distance: 5.8}]->(b)" +
            ", (a)-[:R1 {distance: 4.8}]->(c)" +
            ", (c)-[:R1 {distance: 5.8}]->(d)" +
            ", (d)-[:R1 {distance: 2.6}]->(e)" +
            ", (e)-[:R1 {distance: 5.8}]->(f)" +
            ", (f)-[:R1 {distance: 9.9}]->(g)";
        assertGraphEquals(fromGdl(expectedGraph), subgraph.getGraph("distance"));
    }

    @Test
    void shouldFilterGraph() {
        var config = RandomWalkWithRestartsConfigImpl.builder()
            .startNode(idFunction.of("e"))
            .nodeLabels(List.of("M", "X"))
            .relationshipTypes(List.of("R1"))
            .samplingRatio(0.5)
            .restartProbability(0.1)
            .randomSeed(42L)
            .build();

        var originalIds = List.of(
            idFunction.of("e"),
            idFunction.of("f"),
            idFunction.of("g")
        );

        var rwrGraphConstructor = new GraphSampleConstructor(
            config,
            graphStore,
            new TestNodesSampler(originalIds, nobeLabels(originalIds))
        );

        var subgraph = rwrGraphConstructor.construct();
        assertThat(subgraph.getUnion().nodeCount()).isEqualTo(3);
        var expectedGraph =
            "  (e:M {prop: 50, attr: 48})" +
            ", (f:M {prop: 51, attr: 48})" +
            ", (g:M {prop: 52})" +
            ", (e)-[:R1 {distance: 5.8}]->(f)" +
            ", (f)-[:R1 {distance: 9.9}]->(g)";
        assertGraphEquals(fromGdl(expectedGraph), subgraph.getGraph("distance"));
    }
}
