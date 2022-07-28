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
package org.neo4j.gds.beta.modularity;

import org.assertj.core.data.Offset;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.leiden.ModularityComputer;
import org.neo4j.logging.Log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.ProcedureConstants.TOLERANCE_DEFAULT;

@GdlExtension
class FootballTest {

    private static final String[][] EXPECTED_SEED_COMMUNITIES = {new String[]{"a", "b"}, new String[]{"c", "e"}, new String[]{"d", "f"}};

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    public static final String GRAPH = TestGraphs.FOOTBALL_GRAPH;
    @Inject
    private TestGraph graph;

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;


    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void test(int iterations) {
        ModularityOptimization pmo = compute(graph, iterations, null, 3, 2);
        double modularity1 = pmo.getModularity();
        HugeLongArray communities = HugeLongArray.newArray(graph.nodeCount());
        graph.forEachNode(nodeId -> {
                communities.set(nodeId, pmo.getCommunityId(nodeId));
                return true;
            }
        );
        double modularity2 = ModularityComputer.modularity(graph, communities, 1.0 / graph.relationshipCount());
        assertThat(modularity2).isCloseTo(modularity1, Offset.offset(0.0001));

    }



    @NotNull
    private ModularityOptimization compute(
        Graph graph,
        int maxIterations,
        NodePropertyValues properties,
        int concurrency,
        int minBatchSize
    ) {
        return compute(graph, maxIterations, properties, concurrency, minBatchSize, Neo4jProxy.testLog());
    }

    @NotNull
    private ModularityOptimization compute(
        Graph graph,
        int maxIterations,
        NodePropertyValues properties,
        int concurrency,
        int minBatchSize,
        Log log
    ) {
        var config = ImmutableModularityOptimizationStreamConfig.builder()
            .maxIterations(maxIterations)
            .concurrency(concurrency)
            .batchSize(minBatchSize)
            .build();

        var task = new ModularityOptimizationFactory<>().progressTask(graph, config);
        var progressTracker = new TestProgressTracker(task, log, concurrency, EmptyTaskRegistryFactory.INSTANCE);

        return new ModularityOptimization(
            graph,
            maxIterations,
            TOLERANCE_DEFAULT,
            properties,
            concurrency,
            minBatchSize,
            Pools.DEFAULT,
            progressTracker
        ).compute();
    }

}
