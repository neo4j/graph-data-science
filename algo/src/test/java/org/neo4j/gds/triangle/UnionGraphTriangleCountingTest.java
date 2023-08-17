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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@GdlExtension
class UnionGraphTriangleCountingTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String DB_CYPHER="CREATE "+
        "(a0:L4)," +
        "(a1:L1)," +
        "(a2:L3)," +
        "(a3:L1)," +
        "(a5:L3)," +
        "(a6:L1)," +
        "(a7:L4)," +
        "(a8:L3)," +
        "(a9:L4)," +
        "(a10:L1)," +
        "(a11:L1)," +
        "(a12:L4)," +
        "(a13:L4)," +
        "(a14:L3)," +
        "(a15:L3)," +
        "(a17:L4)," +
        "(a18:L0)," +
        "(a19:L3)," +
        "(a20:L1)," +
        "(a21:L1)," +
        "(a22:L1)," +
        "(a23:L2)," +
        "(a24:L4)," +
        "(a25:L3)," +
        "(a26:L0)," +
        "(a28:L4)," +
        "(a29:L4)," +
        "(a11)-[:T1]->(a15), "+
        "(a0)-[:T1]->(a21), "+
        "(a8)-[:T2]->(a28), "+
        "(a12)-[:T3]->(a12), "+
        "(a9)-[:T4]->(a10), "+
        "(a3)-[:T2]->(a26), "+
        "(a7)-[:T0]->(a21), "+
        "(a11)-[:T3]->(a29), "+
        "(a1)-[:T3]->(a14), "+
        "(a14)-[:T0]->(a22), "+
        "(a10)-[:T1]->(a13), "+
        "(a3)-[:T0]->(a21), "+
        "(a5)-[:T3]->(a28), "+
        "(a10)-[:T3]->(a25), "+
        "(a8)-[:T1]->(a14), "+
        "(a11)-[:T3]->(a15), "+
        "(a13)-[:T2]->(a18), "+
        "(a13)-[:T4]->(a20), "+
        "(a6)-[:T1]->(a29), "+
        "(a12)-[:T3]->(a14), "+
        "(a3)-[:T2]->(a21), "+
        "(a2)-[:T1]->(a21), "+
        "(a0)-[:T0]->(a20), "+
        "(a24)-[:T0]->(a29), "+
        "(a10)-[:T4]->(a19), "+
        "(a0)-[:T1]->(a28), "+
        "(a9)-[:T4]->(a17), "+
        "(a15)-[:T4]->(a21), "+
        "(a21)-[:T1]->(a24) ";

    @Inject
    Graph graph;

    @Test
    void shouldWorkWithUnionGraphs() {
        var config=TriangleCountStreamConfigImpl.builder().concurrency(1).build();
        var a=IntersectingTriangleCount.create(graph,config, Pools.DEFAULT);
        var result=a.compute();
        assertThat(result.globalTriangles()).isEqualTo(0);
    }
}
