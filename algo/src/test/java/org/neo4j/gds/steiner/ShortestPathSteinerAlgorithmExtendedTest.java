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
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
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

    @Test
    void shouldWorkCorrectly() {
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            graph,
            0,
            List.of(2L, 5L),
            1
        ).compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, 0, 1, 2, 3, 4};

        assertThat(steinerTreeResult.relationshipToParentCost().get(5)).isCloseTo(4.1, Offset.offset(1e-5));
        assertThat(steinerTreeResult.parentArray().toArray()).isEqualTo(parentArray);

        assertThat(steinerTreeResult.relationshipToParentCost().get(1)).isEqualTo(1.0);
        assertThat(steinerTreeResult.relationshipToParentCost().get(2)).isEqualTo(1.0);
        assertThat(steinerTreeResult.relationshipToParentCost().get(3)).isEqualTo(1.0);
        assertThat(steinerTreeResult.relationshipToParentCost().get(4)).isEqualTo(1.0);
        assertThat(steinerTreeResult.totalCost()).isCloseTo(8.1, Offset.offset(1e-5));
    }

    @Test
    void shouldWorkCorrectlyWithLineGraph() {
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            lineGraph,
            0,
            List.of(2L, 4L),
            1
        )
            .compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, 0, 1, 2, 3};

        double[] parentCostArray = new double[]{0, 1, 1, 1, 1};

        assertThat(steinerTreeResult.parentArray().toArray()).isEqualTo(parentArray);
        assertThat(steinerTreeResult.relationshipToParentCost().toArray()).isEqualTo(parentCostArray);
        assertThat(steinerTreeResult.totalCost()).isEqualTo(4);
    }


    @Test
    void djikstraShouldWorkCorrectly() {
        var isTerminal = new BitSet(graph.nodeCount());
        isTerminal.set(2);
        isTerminal.set(5);
        var djikstraSteiner = new SteinerBasedDijkstra(
            graph,
            0,
            isTerminal
        );
        var result = djikstraSteiner.compute().pathSet();
        assertThat(result.size()).isEqualTo(2);
        long[][] paths = new long[6][];
        paths[2] = new long[]{0, 1, 2};
        paths[5] = new long[]{2, 3, 4, 5};
        for (PathResult path : result) {
            long targetNode = path.targetNode();
            assertThat(targetNode).isIn(2L, 5L);
            assertThat(path.nodeIds()).isEqualTo(paths[(int) targetNode]);
        }

    }

    @Test
    void shouldWorkIfRevisitsVertices() {
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            extGraph,
            0,
            List.of(2L, 3L),
            1
        ).compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, 0, 1, 6, 1, 4, 5};
        double[] parentCostArray = new double[]{0, 1, 2.1, 8.1, 1, 0.1, 0.1};

        assertThat(steinerTreeResult.parentArray().toArray()).isEqualTo(parentArray);
        for (int i = 0; i < parentCostArray.length; ++i) {
            assertThat(steinerTreeResult.relationshipToParentCost().get(i)).isCloseTo(
                parentCostArray[i],
                Offset.offset(1e-5)
            );
        }

        assertThat(steinerTreeResult.totalCost()).isCloseTo(12.4, Offset.offset(1e-5));

    }

    @Test
    void shouldWorkOnTriangle() {
        var steinerTreeResult = new ShortestPathsSteinerAlgorithm(
            triangleGraph,
            0,
            List.of(1L, 3L),
            1
        ).compute();

        long[] parentArray = new long[]{ShortestPathsSteinerAlgorithm.ROOTNODE, 0, 1, 2};
        double[] parentCostArray = new double[]{0, 15, 3, 6};

        assertThat(steinerTreeResult.parentArray().toArray()).isEqualTo(parentArray);
        assertThat(steinerTreeResult.relationshipToParentCost().toArray()).isEqualTo(parentCostArray);

        assertThat(steinerTreeResult.totalCost()).isEqualTo(24);

    }

}
