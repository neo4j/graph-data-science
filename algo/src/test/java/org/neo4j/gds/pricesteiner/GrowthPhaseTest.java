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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.function.LongToDoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

@GdlExtension
 class GrowthPhaseTest {

    @GdlExtension
    @Nested
    class LineGraph {
        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a1:node)," +
                "  (a2:node)," +
                "  (a3:node)," +
                "  (a4:node)," +
                "(a1)-[:R{w:100.0}]->(a2)," +
                "(a2)-[:R{w:10.0}]->(a3)," +
                "(a3)-[:R{w:100.0}]->(a4)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldFindOptimalSolution() {
            LongToDoubleFunction prizes = (x) -> 20.0;

            var growthPhase = new GrowthPhase(graph, prizes, ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE);
            var result = growthPhase.grow();

            assertThat(result.activeOriginalNodes().get(graph.toMappedNodeId("a1"))).isFalse();
            var a2 = graph.toMappedNodeId("a2");
            var a3= graph.toMappedNodeId("a3");
            assertThat(result.activeOriginalNodes().get(a2)).isTrue();
            assertThat(result.activeOriginalNodes().get(a3)).isTrue();
            assertThat(result.activeOriginalNodes().get(graph.toMappedNodeId("a4"))).isFalse();

            assertThat(result.numberOfTreeEdges()).isEqualTo(1L);

            var treeEdges = result.treeEdges();
            var treeEdgePairs = result.edgeParts();

            var u =- treeEdgePairs.get(2*treeEdges.get(0));
            var v = -treeEdgePairs.get(2*treeEdges.get(0) + 1);
            assertThat(List.of(u,v)).asInstanceOf(LIST).containsExactlyInAnyOrder(a2,a3);

        }

    }

    @Nested
    @GdlExtension
    class HouseGraph{

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a0:node)," +
                "  (a1:node)," +
                "  (a2:node)," +
                "  (a3:node)," +
                "  (a4:node)," +
                "(a0)-[:R{w:10}]->(a1)," +
                "(a0)-[:R{w:72}]->(a3)," +
                "(a1)-[:R{w:74}]->(a2)," +
                "(a1)-[:R{w:62}]->(a3)," +
                "(a1)-[:R{w:54}]->(a4)," +
                "(a2)-[:R{w:15}]->(a3)," +
                "(a2)-[:R{w:62}]->(a4)";


        @Inject
        private  TestGraph graph;

        @Test
        void shouldExecuteGrowthPhaseCorrectly() {
            LongToDoubleFunction prizes = (x) -> 20.0;

            var growthPhase = new GrowthPhase(graph, prizes,ProgressTracker.NULL_TRACKER,TerminationFlag.RUNNING_TRUE);
           var growthResult = growthPhase.grow();
            var clusterStructure = growthPhase.clusterStructure();

            assertThat(clusterStructure.inactiveSince(0)).isEqualTo(5.0);
            assertThat(clusterStructure.inactiveSince(1)).isEqualTo(5.0);

            assertThat(clusterStructure.inactiveSince(2)).isEqualTo(7.5);
            assertThat(clusterStructure.inactiveSince(3)).isEqualTo(7.5);

            assertThat(clusterStructure.inactiveSince(4)).isEqualTo(20);

            assertThat(clusterStructure.inactiveSince(5)).isEqualTo(31.0);
            assertThat(clusterStructure.inactiveSince(6)).isEqualTo(31.0);

            assertThat(clusterStructure.moatAt(0,31)).isEqualTo(5);
            assertThat(clusterStructure.moatAt(1,31)).isEqualTo(5);

            assertThat(clusterStructure.moatAt(2,31)).isEqualTo(7.5);
            assertThat(clusterStructure.moatAt(3,31)).isEqualTo(7.5);

            assertThat(clusterStructure.moatAt(4,31)).isEqualTo(20);

            assertThat(clusterStructure.moatAt(5,31)).isEqualTo(26);
            assertThat(clusterStructure.moatAt(6,31)).isEqualTo(23.5);

            assertThat(clusterStructure.moatAt(7,31)).isEqualTo(0);

            assertThat(clusterStructure.sumOnEdgePart(0,31).cluster()).isEqualTo(7);
            assertThat(clusterStructure.sumOnEdgePart(1,31).cluster()).isEqualTo(7);
            assertThat(clusterStructure.sumOnEdgePart(2,31).cluster()).isEqualTo(7);
            assertThat(clusterStructure.sumOnEdgePart(3,31).cluster()).isEqualTo(7);

            assertThat(clusterStructure.sumOnEdgePart(4,31).cluster()).isEqualTo(4);

            BitSet activeNodes = growthResult.activeOriginalNodes();
            assertThat(activeNodes.cardinality()).isEqualTo(4L);
            assertThat(activeNodes.get(0)).isTrue();
            assertThat(activeNodes.get(1)).isTrue();
            assertThat(activeNodes.get(2)).isTrue();
            assertThat(activeNodes.get(3)).isTrue();

        }

        @Test
        void shouldExecuteGrowthPhaseCorrectlyWithUniqueWeights() {

            HugeLongArray prizes = HugeLongArray.newArray(graph.nodeCount());
            prizes.set(graph.toMappedNodeId("a0"),9);
            prizes.set(graph.toMappedNodeId("a1"),60);
            prizes.set(graph.toMappedNodeId("a2"),30);
            prizes.set(graph.toMappedNodeId("a3"),10);
            prizes.set(graph.toMappedNodeId("a4"),110);

            var growthPhase = new GrowthPhase(
                graph,
                prizes::get,
                ProgressTracker.NULL_TRACKER,
                TerminationFlag.RUNNING_TRUE
            );
            var growthResult = growthPhase.grow();
            var clusterStructure = growthPhase.clusterStructure();


            assertThat(clusterStructure.inactiveSince(0)).isEqualTo(5.0);
            assertThat(clusterStructure.inactiveSince(1)).isEqualTo(5.0);

            assertThat(clusterStructure.inactiveSince(2)).isEqualTo(7.5);
            assertThat(clusterStructure.inactiveSince(3)).isEqualTo(7.5);

            assertThat(clusterStructure.inactiveSince(4)).isEqualTo(27);
            assertThat(clusterStructure.inactiveSince(5)).isEqualTo(27);

            assertThat(clusterStructure.inactiveSince(6)).isEqualTo(31);
            assertThat(clusterStructure.inactiveSince(7)).isEqualTo(31);


            assertThat(clusterStructure.active(8)).isTrue();

            assertThat(clusterStructure.moatAt(0,31)).isEqualTo(5);
            assertThat(clusterStructure.moatAt(1,31)).isEqualTo(5);

            assertThat(clusterStructure.moatAt(2,31)).isEqualTo(7.5);
            assertThat(clusterStructure.moatAt(3,31)).isEqualTo(7.5);

            assertThat(clusterStructure.moatAt(4,31)).isEqualTo(27);

            assertThat(clusterStructure.moatAt(5,31)).isEqualTo(22);
            assertThat(clusterStructure.moatAt(6,31)).isEqualTo(23.5);
            assertThat(clusterStructure.moatAt(7,31)).isEqualTo(4);


            for (long u=0;u< graph.nodeCount();++u){
                assertThat(clusterStructure.sumOnEdgePart(u,31))
                    .satisfies( clusterMoatPair->{
                        assertThat(clusterMoatPair.totalMoat()).isEqualTo(31);
                        assertThat(clusterMoatPair.cluster()).isEqualTo(8);
                    });
            }


            BitSet activeNodes = growthResult.activeOriginalNodes();

            assertThat(activeNodes.cardinality()).isEqualTo(5L);

            var treeEdges = growthResult.treeEdges();
            var treeEdgeWeights = growthResult.edgeCosts();

            LongToDoubleFunction  costSupplier  =  e ->  treeEdgeWeights.get(treeEdges.get(e));
            assertThat(List.of(costSupplier.applyAsDouble(0),
                costSupplier.applyAsDouble(1),
                costSupplier.applyAsDouble(2),
                costSupplier.applyAsDouble(3))).asInstanceOf(LIST).containsExactlyInAnyOrder(10.0,62.0,54.0,15.0);

        }
    }


}
