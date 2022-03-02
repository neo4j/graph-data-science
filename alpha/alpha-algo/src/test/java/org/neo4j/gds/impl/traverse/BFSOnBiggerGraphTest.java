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
package org.neo4j.gds.impl.traverse;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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

    @GdlGraph
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
    @ValueSource(ints = {1, 4})
    void testBfsToTargetOut(int concurrency) {
        long source = graph.toMappedNodeId("a");
        long target = graph.toMappedNodeId("r");
        long[] nodes = new BFS(
            graph,
            source,
            (s, t, w) -> t == target ? ExitPredicate.Result.BREAK : ExitPredicate.Result.FOLLOW,
            (s, t, w) -> 1.,
            concurrency,
            ProgressTracker.NULL_TRACKER,
            1
        ).compute();


        assertContains(graph, new String[]{
            "a",                // start node
            "b",                // layer 1
            "c", "l",           // layer 2
            "d", "f", "m", "p", // layer 3
            "g", "i", "n", "q", // layer 4
            "e", "h", "j",      // layer 5
            "o", "r"            // layer 6
        }, nodes);
    }

    void assertContains(TestGraph graph, String[] expected, long[] given) {
        Arrays.sort(given);
        assertEquals(
            expected.length,
            given.length,
            "expected " + Arrays.toString(expected) + " | given " + Arrays.toString(given)
        );

        for (String ex : expected) {
            final long id = graph.toMappedNodeId(ex);
            if (Arrays.binarySearch(given, id) == -1) {
                fail(ex + " not in " + Arrays.toString(expected));
            }
        }
    }

}
