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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.function.LongToDoubleFunction;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class PCSTFastTest {

    @Nested
    @GdlExtension
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

            var pcst = new PCSTFast(graph, prizes, ProgressTracker.NULL_TRACKER);
            var result = pcst.compute();

            var parentArray = result.parentArray();

            assertThat(parentArray.get(graph.toMappedNodeId("a1"))).isEqualTo(PriceSteinerTreeResult.PRUNED);
            assertThat(parentArray.get(graph.toMappedNodeId("a4"))).isEqualTo(PriceSteinerTreeResult.PRUNED);

            boolean par1 = (parentArray.get(graph.toMappedNodeId("a2")) == graph.toMappedNodeId("a3")) && (parentArray.get(
                graph.toMappedNodeId("a3")) == PriceSteinerTreeResult.ROOT);
            boolean par2 = (parentArray.get(graph.toMappedNodeId("a3")) == graph.toMappedNodeId("a2")) && (parentArray.get(
                graph.toMappedNodeId("a2")) == PriceSteinerTreeResult.ROOT);
            assertThat(par1 ^ par2).isTrue();


        }
    }
}
