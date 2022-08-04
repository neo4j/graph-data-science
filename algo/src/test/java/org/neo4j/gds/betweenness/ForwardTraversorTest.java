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
package org.neo4j.gds.betweenness;

import com.carrotsearch.hppc.LongArrayList;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

@GdlExtension
class ForwardTraversorTest {

    @GdlGraph(graphNamePrefix = "equallyWeighted")
    private static final String equallyWeightedGdl =
        "CREATE " +
        "  (a1)-[:REL {weight: 1.0}]->(b)" +
        ", (a2)-[:REL {weight: 1.0}]->(b)" +
        ", (b) -[:REL {weight: 1.0}]->(c)" +
        ", (b) -[:REL {weight: 1.0}]->(d)" +
        ", (c) -[:REL {weight: 1.0}]->(e)" +
        ", (d) -[:REL {weight: 1.0}]->(e)" +
        ", (e) -[:REL {weight: 1.0}]->(f)";

    @Inject
    private Graph equallyWeightedGraph;

    @GdlGraph(graphNamePrefix = "weighted")
    private static final String weightedGdl =
        "CREATE " +
        "  (a1)-[:REL {weight: 1.0}]->(b)" +
        ", (a2)-[:REL {weight: 1.0}]->(b)" +
        ", (b) -[:REL {weight: 4.0}]->(c)" +
        ", (b) -[:REL {weight: 1.3}]->(d)" +
        ", (c) -[:REL {weight: 1.0}]->(e)" +
        ", (d) -[:REL {weight: 0.2}]->(e)" +
        ", (e) -[:REL {weight: 0.001}]->(f)";
    @Inject
    private Graph weightedGraph;

    @Inject
    private IdFunction weightedIdFunction;


    @Test
    void shouldWorkOnUnWeightedGraphs() {
        var backwardNodes = HugeLongArrayStack.newStack(equallyWeightedGraph.nodeCount());
        var predecessors = HugeObjectArray.newArray(LongArrayList.class, equallyWeightedGraph.nodeCount());
        var sigma = HugeLongArray.newArray(equallyWeightedGraph.nodeCount());
        UnweightedForwardTraversor unweightedForwardTraversor = UnweightedForwardTraversor.create(
            equallyWeightedGraph,
            predecessors,
            backwardNodes,
            sigma,
            TerminationFlag.RUNNING_TRUE
        );
        unweightedForwardTraversor.clear();


        sigma.addTo(0, 1);
        unweightedForwardTraversor.traverse(0);

        SoftAssertions softAssertions = new SoftAssertions();

        softAssertions.assertThat(sigma.toArray()).isEqualTo(new long[]{1, 1, 0, 1, 1, 2, 2});
        softAssertions.assertThat(predecessors.get(0)).isNull();
        softAssertions.assertThat(predecessors.get(1).toArray()).containsExactly(0);
        softAssertions.assertThat(predecessors.get(2)).isNull();
        softAssertions.assertThat(predecessors.get(3).toArray()).containsExactly(1);
        softAssertions.assertThat(predecessors.get(4).toArray()).containsExactly(1);
        softAssertions.assertThat(predecessors.get(5).toArray()).containsExactly(3, 4);
        softAssertions.assertThat(predecessors.get(6).toArray()).containsExactly(5);

        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(6);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(5);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(4);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(3);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(1);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(0);

        softAssertions.assertThat(backwardNodes.isEmpty()).isTrue();

        softAssertions.assertAll();
    }

    @Test
    void shouldWorkOnWeightedGraphs() {
        var backwardNodes = HugeLongArrayStack.newStack(equallyWeightedGraph.nodeCount());
        var predecessors = HugeObjectArray.newArray(LongArrayList.class, equallyWeightedGraph.nodeCount());
        var sigma = HugeLongArray.newArray(equallyWeightedGraph.nodeCount());
        WeightedForwardTraversor weightedForwardTraversor = WeightedForwardTraversor.create(
            weightedGraph,
            predecessors,
            backwardNodes,
            sigma,
            TerminationFlag.RUNNING_TRUE
        );
        weightedForwardTraversor.clear();


        sigma.addTo(0, 1);
        weightedForwardTraversor.traverse(0);

        SoftAssertions softAssertions = new SoftAssertions();

        softAssertions.assertThat(sigma.toArray()).isEqualTo(new long[]{1, 1, 0, 1, 1, 1, 1});
        softAssertions.assertThat(predecessors.get(0)).isNull();
        softAssertions.assertThat(predecessors.get(1).toArray()).containsExactly(0);
        softAssertions.assertThat(predecessors.get(2)).isNull();
        softAssertions.assertThat(predecessors.get(3).toArray()).containsExactly(1);
        softAssertions.assertThat(predecessors.get(4).toArray()).containsExactly(1);
        softAssertions.assertThat(predecessors.get(5).toArray()).containsExactly(4);
        softAssertions.assertThat(predecessors.get(6).toArray()).containsExactly(5);

        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(3);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(6);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(5);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(4);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(1);
        softAssertions.assertThat(backwardNodes.pop()).isEqualTo(0);

        softAssertions.assertThat(backwardNodes.isEmpty()).isTrue();

        softAssertions.assertAll();
    }


}
