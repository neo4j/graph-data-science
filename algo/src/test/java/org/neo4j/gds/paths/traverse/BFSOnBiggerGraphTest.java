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

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.crossArguments;

/*

    (a)->(b)->(l)->(p)->(q)-->(r)->(s)
           \    \            / |\
            \    (m)->(n)->(o) | \
             \                 |  \
             (c)->(f)->(i)--->(j)->(k)
               \    \        / |
                \    (g)-->(h) |
                 \             |
                  (d)--------->(e)
*/

@GdlExtension
class BFSOnBiggerGraphTest {

    @GdlGraph(idOffset = 0)
    private static final String CYPHER =
        "CREATE " +
        "  (a:Node { num: 1})" +
        ", (b:Node { num: 2})" +
        ", (c:Node { num: 3})" +
        ", (d:Node { num: 4})" +
        ", (e:Node { num: 5})" +
        ", (f:Node { num: 6})" +
        ", (g:Node { num: 7})" +
        ", (h:Node { num: 8})" +
        ", (i:Node { num: 9})" +
        ", (j:Node { num: 10})" +
        ", (k:Node { num: 11})" +
        ", (l:Node { num: 12})" +
        ", (m:Node { num: 13})" +
        ", (n:Node { num: 14})" +
        ", (o:Node { num: 15})" +
        ", (p:Node { num: 16})" +
        ", (q:Node { num: 17})" +
        ", (r:Node { num: 18})" +
        ", (s:Node { num: 19})" +

        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(l)" +
        ", (l)-[:REL]->(m)" +
        ", (l)-[:REL]->(p)" +
        ", (p)-[:REL]->(q)" +
        ", (q)-[:REL]->(r)" +
        ", (m)-[:REL]->(n)" +
        ", (n)-[:REL]->(o)" +
        ", (o)-[:REL]->(r)" +
        ", (r)-[:REL]->(s)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(d)" +
        ", (c)-[:REL]->(f)" +
        ", (d)-[:REL]->(e)" +
        ", (e)-[:REL]->(j)" +
        ", (j)-[:REL]->(k)" +
        ", (k)-[:REL]->(r)" +
        ", (f)-[:REL]->(g)" +
        ", (g)-[:REL]->(h)" +
        ", (h)-[:REL]->(j)" +
        ", (f)-[:REL]->(i)" +
        ", (i)-[:REL]->(j)";

    @Inject
    private static TestGraph graph;

    @ParameterizedTest
    @MethodSource("bfsParameters")
    void testBfsToTargetOut(int concurrency, int delta) {
        long source = graph.toMappedNodeId("a");
        long target = graph.toMappedNodeId("r");
        long[] nodes = BFS.create(
            graph,
            source,
            (s, t, w) -> t == target ? ExitPredicate.Result.BREAK : ExitPredicate.Result.FOLLOW,
            (s, t, w) -> 1.,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            delta,
            BFS.ALL_DEPTHS_ALLOWED
        ).compute().toArray();

        assertThat(nodes)
            .isEqualTo(Stream.of(
                "a",                        // start node
                "b",                        // layer 1
                "c", "l",                   // layer 2
                "d", "f", "m", "p",         // layer 3
                "e", "g", "i", "n", "q",    // layer 4
                "j", "h",                   // layer 5
                "o", "r"                    // layer 6
            ).mapToLong(graph::toMappedNodeId).toArray());
    }

    static Stream<Arguments> bfsParameters() {
        return crossArguments(
            () -> Stream.of(Arguments.of(1), Arguments.of(4), Arguments.of(8)), // concurrencies
            () -> Stream.of(Arguments.of(1), Arguments.of(3), Arguments.of(5), Arguments.of(64))  // deltas
        );
    }
}
