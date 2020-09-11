/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.triangle.ImmutableTriangleCountBaseConfig;
import org.neo4j.graphalgo.triangle.IntersectingTriangleCountFactory;
import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class TriangleCountBug {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (n0)" +
        ", (n1)" +
        ", (n2)" +
        ", (n3)" +
        ", (n4)" +
        ", (n5)" +
        ", (n6)" +
        ", (n7)" +
        ", (n0)-->(n1)" +
        ", (n0)-->(n2)" +
        ", (n0)-->(n3)" +
        ", (n0)-->(n4)" +
        ", (n0)-->(n6)" +
        ", (n0)-->(n7)" +
        ", (n1)-->(n0)" +
        ", (n1)-->(n2)" +
        ", (n1)-->(n3)" +
        ", (n1)-->(n5)" +
        ", (n1)-->(n6)" +
        ", (n1)-->(n7)" +
        ", (n2)-->(n0)" +
        ", (n2)-->(n1)" +
        ", (n2)-->(n3)" +
        ", (n3)-->(n0)" +
        ", (n3)-->(n1)" +
        ", (n3)-->(n2)" +
        ", (n3)-->(n5)" +
        ", (n3)-->(n6)" +
        ", (n3)-->(n7)" +
        ", (n4)-->(n0)" +
        ", (n5)-->(n1)" +
        ", (n5)-->(n3)" +
        ", (n6)-->(n0)" +
        ", (n6)-->(n1)" +
        ", (n6)-->(n3)" +
        ", (n7)-->(n0)" +
        ", (n7)-->(n1)" +
        "  (n7)-->(n3)";

    @Inject
    private Graph graph;

    @Test
    void producesWrongResult() {
        var config = ImmutableTriangleCountBaseConfig
            .builder()
            .concurrency(1)
            .build();

        var result = new IntersectingTriangleCountFactory<>()
            .build(graph, config, AllocationTracker.empty(), NullLog.getInstance())
            .compute();

        System.out.println("result.globalTriangles() = " + result.globalTriangles());

        System.out.println("single stuff:");
        for (int i = 0; i < graph.nodeCount(); i++) {
            System.out.print(result.localTriangles().get(i) + ",");
        }

        System.out.println("Triangles");
        var explicitTriangles = new TriangleStream(graph, Pools.DEFAULT, 1).compute();
        explicitTriangles.forEach(triangle -> System.out.println(triangle.toString()));

        System.out.println();
        System.out.println("0->3:" + graph.exists(0, 3));
        System.out.println("0<-3:" + graph.exists(3, 0));
        System.out.println("3->6:" + graph.exists(3, 6));
        System.out.println("3<-6:" + graph.exists(6, 3));
        System.out.println("6->0:" + graph.exists(6, 0));
        System.out.println("6<-0:" + graph.exists(0, 6));

        assertEquals(11, result.globalTriangles());
    }
}
