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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.paths.PathResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ShortestPathSteinerAlgorithmExtendedTest {

    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +
        "  (a5:Node)," +


        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 6.2}]->(a5)," +

        "  (a1)-[:R {weight: 1.0}]->(a2)," +

        "  (a2)-[:R {weight: 1.0}]->(a3)," +

        "  (a3)-[:R {weight: 1.0}]->(a4)," +

        "  (a4)-[:R {weight: 4.1}]->(a5)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;


    @GdlGraph(graphNamePrefix = "line", orientation = Orientation.NATURAL)
    private static final String lineQuery =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +


        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a1)-[:R {weight: 1.0}]->(a2)," +

        "  (a2)-[:R {weight: 1.0}]->(a3)," +

        "  (a3)-[:R {weight: 1.0}]->(a4)";


    @Inject
    private TestGraph lineGraph;

    @Inject
    private IdFunction lineIdFunction;


    @GdlGraph(graphNamePrefix = "ext", orientation = Orientation.NATURAL)
    private static final String extQuery =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +
        "  (a5:Node)," +
        "  (a6:Node)," +


        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 10.0}]->(a3)," +
        "  (a1)-[:R {weight: 2.1}]->(a2)," +
        "  (a1)-[:R {weight: 1.0}]->(a4)," +
        "  (a4)-[:R {weight: 0.1}]->(a5)," +
        "  (a5)-[:R {weight: 0.1}]->(a6)," +
        "  (a6)-[:R {weight: 8.1}]->(a3)";


    @Inject
    private TestGraph extGraph;

    @Inject
    private IdFunction extIdFunction;

    @GdlGraph(graphNamePrefix = "triangle", orientation = Orientation.NATURAL)
    private static final String triangle =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +

        "  (a0)-[:R {weight: 15.0}]->(a1)," +
        "  (a0)-[:R {weight: 10.0}]->(a2)," +
        "  (a1)-[:R {weight: 3.0}]->(a2)," +
        "  (a2)-[:R {weight: 6.0}]->(a3)";

    @Inject
    private TestGraph triangleGraph;

    @Inject
    private IdFunction triangleIdFunction;

    @Test
    void shouldWorkCorrectly() {

        var a = SteinerTestUtils.getNodes(idFunction, 6);
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            graph,
            a[0],
            List.of(a[2], a[5]),
            2.0,
            1,
            false
        ).compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, a[0], a[1], a[2], a[3], a[4]};
        double[] parentCostArray = new double[]{0, 1.0, 1.0, 1.0, 1.0, 4.1};
        SteinerTestUtils.assertTreeIsCorrect(idFunction, steinerTreeResult, parentArray, parentCostArray, 8.1);
    }

    @Test
    void shouldWorkCorrectlyWithLineGraph() {

        var a = SteinerTestUtils.getNodes(lineIdFunction, 5);
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            lineGraph,
            a[0],
            List.of(a[2], a[4]),
            2.0,
            1,
            false
        )
            .compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, a[0], a[1], a[2], a[3]};

        double[] parentCostArray = new double[]{0, 1, 1, 1, 1};

        SteinerTestUtils.assertTreeIsCorrect(lineIdFunction, steinerTreeResult, parentArray, parentCostArray, 4);
    }


    @Test
    void deltaSteppingShouldWorkCorrectly() {
        var a = SteinerTestUtils.getNodes(idFunction, 6);
        var isTerminal = new BitSet(graph.nodeCount());
        isTerminal.set(a[2]);
        isTerminal.set(a[5]);
        var deltaSteiner = new SteinerBasedDeltaStepping(
            graph,
            0,
            2.0,
            isTerminal,
            1,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        var result = deltaSteiner.compute().pathSet();
        assertThat(result.size()).isEqualTo(2);
        long[][] paths = new long[6][];
        paths[(int) a[2]] = new long[]{a[0], a[1], a[2]};
        paths[(int) a[5]] = new long[]{a[2], a[3], a[4], a[5]};
        for (PathResult path : result) {
            long targetNode = path.targetNode();
            assertThat(targetNode).isIn(a[2], a[5]);
            assertThat(path.nodeIds()).isEqualTo(paths[(int) a[(int) targetNode]]);
        }

    }

    @Test
    void shouldWorkIfRevisitsVertices() {
        var a = SteinerTestUtils.getNodes(extIdFunction, 7);
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            extGraph,
            a[0],
            List.of(a[2], a[3]),
            2.0,
            1,
            false
        ).compute();

        long[] parentArray = new long[]{
            ShortestPathsSteinerAlgorithm.ROOTNODE,
            a[0],
            a[1],
            a[6],
            a[1],
            a[4],
            a[5]
        };
        double[] parentCostArray = new double[]{0, 1, 2.1, 8.1, 1, 0.1, 0.1};

        SteinerTestUtils.assertTreeIsCorrect(extIdFunction, steinerTreeResult, parentArray, parentCostArray, 12.4);

    }

    @Test
    void shouldWorkOnTriangle() {
        var a = SteinerTestUtils.getNodes(triangleIdFunction, 4);
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            triangleGraph,
            a[0],
            List.of(a[1], a[3]),
            2.0,
            1,
            false
        ).compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, a[0], a[1], a[2]};
        double[] parentCostArray = new double[]{0, 15, 3, 6};

        SteinerTestUtils.assertTreeIsCorrect(triangleIdFunction, steinerTreeResult, parentArray, parentCostArray, 24);

    }

}
