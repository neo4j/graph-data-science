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
package org.neo4j.gds.paths.yens;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
public class PeekPruningUndirectedTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:A {id: 0})" +
            ", (b:B {id: 1})" +
            ", (c:C {id: 2})" +
            ", (d:D {id: 3})" +
            ", (z:Z {id: 4})" +
            ", (b)-[:REL {cost: 8.0}]->(a)" +
            ", (c)-[:REL {cost: 1.0}]->(a)" +
            ", (c)-[:REL {cost: 6.0}]->(b)" +
            ", (d)-[:REL {cost: 1.0}]->(c)" +
            ", (d)-[:REL {cost: 1.0}]->(a)" +
            ", (b)-[:REL {cost: 1.0}]->(d)";

    @Inject
    private TestGraph graph;

    @Test
    void testUndirected() {
        int k = 2;
        long nodeCount = graph.nodeCount();
        long sourceNode = graph.toMappedNodeId("a");
        long targetNode = graph.toMappedNodeId("b");
        var concurrency = new Concurrency(1);
        var executorService = DefaultPool.INSTANCE;
        var terminationFlag = TerminationFlag.RUNNING_TRUE;

        var paths = PeekPruning.pathsAndReachability(
            graph,
            sourceNode,
            targetNode,
            PeekPruning.deltaStep(concurrency, executorService, ProgressTracker.NULL_TRACKER, terminationFlag));

        assertThat(paths.reachable().get(graph.toMappedNodeId("z"))).isEqualTo(false);
        List.of("a", "b", "c", "d").forEach(name -> {
            assertThat(paths.reachable().get(graph.toMappedNodeId(name))).isEqualTo(true);
        });

        var yensParameters = new YensParameters(
            graph.toOriginalNodeId(sourceNode),
            graph.toOriginalNodeId(targetNode),
            k,
            concurrency
            );
        var prunedYen = YensFactory.create(
            graph,
            yensParameters,
            executorService,
            ProgressTracker.NULL_TRACKER,
            terminationFlag
        );

        assertThat(prunedYen).isInstanceOf(PeekPruningYens.class);

        Function<Stream<String>, long[]> mapIds = stream -> stream.mapToLong(graph::toMappedNodeId).toArray();

        var prunedPaths = prunedYen.compute().paths();
        var pathNodeIds = prunedPaths.map(PathResult::nodeIds).toArray(long[][]::new);
        assertThat(pathNodeIds.length).isEqualTo(2);
        assertThat(pathNodeIds[0]).containsExactly(mapIds.apply(Stream.of("a", "d", "b")));
        assertThat(pathNodeIds[1]).containsExactly(mapIds.apply(Stream.of("a", "c", "d", "b")));
    }
}
