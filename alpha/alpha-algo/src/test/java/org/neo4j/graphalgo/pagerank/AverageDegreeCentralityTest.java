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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
final class AverageDegreeCentralityTest {

    @GdlGraph(graphNamePrefix = "natural", orientation = Orientation.NATURAL)
    @GdlGraph(graphNamePrefix = "reverse", orientation = Orientation.REVERSE)
    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED, aggregation = Aggregation.SINGLE)
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Label1)" +
            ", (b:Label1)" +
            ", (c:Label1)" +
            ", (d:Label1)" +
            ", (e:Label1)" +
            ", (f:Label1)" +
            ", (g:Label1)" +
            ", (h:Label1)" +
            ", (i:Label1)" +
            ", (j:Label1)" +

            ", (b)-[:TYPE1 {weight: 2.0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (d)-[:TYPE1 {weight: 2.0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 2.0}]->(b)" +

            ", (e)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 2.0}]->(f)" +

            ", (f)-[:TYPE1 {weight: 2.0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 2.0}]->(e)";


    @Inject
    private Graph naturalGraph;

    @Inject
    private Graph reverseGraph;

    @Inject
    private Graph undirectedGraph;

    @Test
    void averageOutgoingCentrality() {
        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(naturalGraph, Pools.DEFAULT, 4);
        degreeCentrality.compute();

        assertEquals(0.9, degreeCentrality.average(), 0.01);
    }

    @Test
    void averageIncomingCentrality() {
        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(reverseGraph, Pools.DEFAULT, 4);
        degreeCentrality.compute();

        assertEquals(0.9, degreeCentrality.average(), 0.01);
    }

    @Test
    void totalCentrality() {
        AverageDegreeCentrality degreeCentrality = new AverageDegreeCentrality(undirectedGraph, Pools.DEFAULT, 4);
        degreeCentrality.compute();

        assertEquals(1.4, degreeCentrality.average(), 0.01);
    }
}
