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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.crossArguments;

@GdlExtension
class BFSComplexTreeTest {

    @GdlGraph(idOffset = 0)
    private static final String CYPHER =
        "CREATE " +
        "  (a:Node)" +

        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +

        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (j:Node)" +
        ", (k:Node)" +
        ", (l:Node)" +
        ", (m:Node)" +

        ", (a)-[:REL]->(b)" +
        ", (a)-[:REL]->(c)" +
        ", (a)-[:REL]->(d)" +
        ", (a)-[:REL]->(e)" +
        ", (a)-[:REL]->(f)" +

        ", (b)-[:REL]->(g)" +
        ", (b)-[:REL]->(h)" +
        ", (b)-[:REL]->(i)" +
        ", (b)-[:REL]->(j)" +
        ", (b)-[:REL]->(k)" +
        ", (b)-[:REL]->(l)" +

        ", (c)-[:REL]->(g)" +
        ", (c)-[:REL]->(h)" +
        ", (c)-[:REL]->(i)" +
        ", (c)-[:REL]->(j)" +
        ", (c)-[:REL]->(k)" +
        ", (c)-[:REL]->(l)" +

        ", (d)-[:REL]->(g)" +
        ", (d)-[:REL]->(h)" +
        ", (d)-[:REL]->(i)" +
        ", (d)-[:REL]->(j)" +
        ", (d)-[:REL]->(k)" +
        ", (d)-[:REL]->(l)" +

        ", (e)-[:REL]->(g)" +
        ", (e)-[:REL]->(h)" +
        ", (e)-[:REL]->(i)" +
        ", (e)-[:REL]->(j)" +
        ", (e)-[:REL]->(k)" +
        ", (e)-[:REL]->(l)" +

        ", (f)-[:REL]->(m)";

    @Inject
    private static TestGraph graph;

    @ParameterizedTest
    @MethodSource("bfsParameters")
    void testBfsToTargetOut(int concurrency, int delta) {
        long source = graph.toMappedNodeId("a");
        List<Long> targets = List.of(
            graph.toMappedNodeId("h"),
            graph.toMappedNodeId("j"),
            graph.toMappedNodeId("l")
        );
        long[] nodes = BFS.create(
            graph,
            source,
            (s, t, w) -> targets.contains(t) ? ExitPredicate.Result.BREAK : ExitPredicate.Result.FOLLOW,
            (s, t, w) -> 1.,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            delta,
            BFS.ALL_DEPTHS_ALLOWED
        ).compute().toArray();

        assertThat(nodes)
            .isEqualTo(Stream.of(
                "a",
                "b", "c", "d", "e", "f",
                "g", "h"
            ).mapToLong(graph::toMappedNodeId).toArray());
    }

    static Stream<Arguments> bfsParameters() {
        return crossArguments(
            () -> Stream.of(Arguments.of(1), Arguments.of(4), Arguments.of(8)), // concurrencies
            () -> Stream.of(Arguments.of(1), Arguments.of(3), Arguments.of(5), Arguments.of(64))  // deltas
        );
    }

}
