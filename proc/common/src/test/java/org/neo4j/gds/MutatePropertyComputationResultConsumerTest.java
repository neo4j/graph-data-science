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
package org.neo4j.gds;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.test.ImmutableTestMutateConfig;
import org.neo4j.gds.test.TestAlgoResultBuilder;
import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestMutateConfig;
import org.neo4j.gds.test.TestProc;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class MutatePropertyComputationResultConsumerTest {

    @GdlGraph
    static final String DB_CYPHER = "(a {prop: 42}), (b {prop: 1337})";

    @Inject
    private GraphStore graphStore;

    MutatePropertyComputationResultConsumer<TestAlgorithm, TestAlgorithm, TestMutateConfig, TestProc.TestResult> mutateResultConsumer;

    @BeforeEach
    void setup() {
        mutateResultConsumer = new MutatePropertyComputationResultConsumer<>(
            (computationResult, resultProperty, allocationTracker) -> new TestNodeProperties(),
            computationResult -> new TestAlgoResultBuilder(),
            new TestLog(),
            AllocationTracker.empty()
        );
    }

    @Test
    void shouldMutateNodeProperties() {
        var computationResult = getComputationResult(
            graphStore,
            graphStore.getUnion(),
            ImmutableTestMutateConfig.builder().mutateProperty("mutated").build()
        );

        mutateResultConsumer.consume(computationResult);

        assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "mutated")).isTrue();
        var mutatedNodeProperty = graphStore.nodeProperty("mutated");

        assertThat(mutatedNodeProperty.propertyState()).isEqualTo(PropertyState.TRANSIENT);

        var mutatedNodePropertyValues = mutatedNodeProperty.values();
        assertThat(mutatedNodePropertyValues.longValue(0)).isEqualTo(0);
        assertThat(mutatedNodePropertyValues.longValue(1)).isEqualTo(1);
    }


    @Test
    void testGraphMutationOnFilteredGraph() {
        GdlFactory gdlFactory = GdlFactory
            .builder()
            .graphName("graph")
            .gdlGraph("CREATE (b: B), (a1: A), (a2: A), (a1)-[:REL]->(a2)")
            .build();

        CSRGraphStore graphStore = gdlFactory.build().graphStore();

        CSRGraph filteredGraph = graphStore.getGraph(
            NodeLabel.listOf("A"),
            RelationshipType.listOf("REL"),
            Optional.empty()
        );

        String mutateProperty = "mutated";

        var computationResult = getComputationResult(
            graphStore,
            filteredGraph,
            ImmutableTestMutateConfig.builder().addNodeLabel("A").mutateProperty(mutateProperty).build()
        );

        mutateResultConsumer.consume(computationResult);

        assertThat(graphStore.hasNodeProperty(NodeLabel.of("A"), mutateProperty)).isTrue();
        assertThat(graphStore.hasNodeProperty(NodeLabel.of("B"), mutateProperty)).isFalse();

        NodeProperties mutatedProperty = graphStore.nodeProperty(mutateProperty).values();
        assertThat(mutatedProperty.longValue(gdlFactory.nodeId("b"))).isEqualTo(DefaultValue.LONG_DEFAULT_FALLBACK);
        assertThat(mutatedProperty.longValue(gdlFactory.nodeId("a1"))).isEqualTo(0);
        assertThat(mutatedProperty.longValue(gdlFactory.nodeId("a2"))).isEqualTo(1);
    }

    @NotNull
    private AlgoBaseProc.ComputationResult<TestAlgorithm, TestAlgorithm, TestMutateConfig> getComputationResult(
        GraphStore graphStore,
        Graph graph,
        TestMutateConfig config
    ) {
        TestAlgorithm algorithm = new TestAlgorithm(
            graph,
            AllocationTracker.empty(),
            Long.MAX_VALUE,
            ProgressTracker.EmptyProgressTracker.NULL_TRACKER,
            false
        );

        ImmutableComputationResult.Builder<TestAlgorithm, TestAlgorithm, TestMutateConfig> builder = ImmutableComputationResult.builder();

        return builder
            .algorithm(algorithm)
            .config(config)
            .result(algorithm)
            .graph(graph)
            .graphStore(graphStore)
            .createMillis(0)
            .computeMillis(0)
            .build();
    }

    class TestNodeProperties implements LongNodeProperties {

        @Override
        public long size() {
            return graphStore.nodeCount();
        }

        @Override
        public long longValue(long nodeId) {
            return nodeId;
        }
    }

}