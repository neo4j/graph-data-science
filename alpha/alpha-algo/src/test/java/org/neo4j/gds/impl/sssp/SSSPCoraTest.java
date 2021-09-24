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
package org.neo4j.gds.impl.sssp;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoTestBase;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.datasets.CommunityDbCreator;
import org.neo4j.gds.datasets.Cora;
import org.neo4j.gds.datasets.CoraSchema;
import org.neo4j.gds.datasets.DatasetManager;
import org.neo4j.gds.junit.annotation.Edition;
import org.neo4j.gds.junit.annotation.GdsEditionTest;
import org.neo4j.gds.paths.dijkstra.Dijkstra;
import org.neo4j.gds.paths.dijkstra.config.ImmutableAllShortestPathsDijkstraStreamConfig;

import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdsEditionTest(Edition.EE)
public class SSSPCoraTest extends AlgoTestBase {

    private static Graph graph;

    @BeforeAll
    static void setup(@TempDir Path tempDir) {
        DatasetManager datasetManager = new DatasetManager(tempDir, CommunityDbCreator.getInstance());
        var cora = datasetManager.openDb(Cora.ID);

        graph = new StoreLoaderBuilder()
            .api(cora)
            .addNodeLabel(CoraSchema.PAPER_LABEL.name())
            .addRelationshipType(CoraSchema.CITES_TYPE).build().graph();

        cora.shutdown();

        assertEquals(2708, graph.nodeCount());
        assertEquals(5429, graph.relationshipCount());
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.impl.sssp.DeltaSteppingTest#deltaAndConcurrency")
    void deltaEqualsDijkstraSSSP(double delta, int concurrency) {
        // Path stats: 1104 target nodes, average length = 6.973732, max length = 15
        var startNode = 42L;

        DeltaStepping.DeltaSteppingResult deltaSteppingResult = new DeltaStepping(
            graph,
            startNode,
            delta,
            concurrency,
            Pools.DEFAULT,
            AllocationTracker.empty()
        ).compute();

        var dijkstraResult = Dijkstra.singleSource(
            graph,
            ImmutableAllShortestPathsDijkstraStreamConfig.builder()
                .concurrency(1)
                .sourceNode(startNode)
                .trackRelationships(false)
                .build(),
            Optional.empty(),
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        ).compute().pathSet();

        assertThat(dijkstraResult.size()).isEqualTo(deltaSteppingResult.numberOfTargetNodes());
        dijkstraResult.forEach(path -> assertThat(path.totalCost()).isEqualTo(deltaSteppingResult.distance(path.targetNode())));
    }
}
