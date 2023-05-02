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
package org.neo4j.gds.beta.closeness;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Graph:
 *
 * (A) <->  (B)  <- (C)
 * (D) <->  (E) <- (F)
 *
 * Calculation:
 *
 * standard:
 *
 * d(A,B)=1
 * d(C,B)=1  farness(B)=2  component(B)=2  CC(B)=1
 * d(B,A)=1
 * d(C,A)=2  farness(A)= 3 component(A)=2 CC(A)=2/3
 *
 * d(A,C)=inf
 * d(B,C)=inf farness(C)=inf, comp(C)=0  CC(C)=0
 *
 * D,E,F follow suit
 *
 * with WF:
 *
 * CCWF(B) = (4/5) * (1/2) =  2/5
 * CCWF(A) =  (4/5) * (1/3) = 4/15
 * CCWF(C) = 0
 *
 * D,E,F follow suit
 */
@GdlExtension
class ClosenessCentralityDirectedTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +

        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(a)" +
        ", (c)-[:TYPE]->(b)" +
        ", (d)-[:TYPE]->(e)" +
        ", (e)-[:TYPE]->(d)" +
        ", (f)-[:TYPE]->(e)";

    @Inject
    private TestGraph graph;

    @Test
    void testCentrality() {
        IdFunction idFunction = graph::toMappedNodeId;

        var algo = ClosenessCentrality.of(
            graph,
            ImmutableClosenessCentralityStreamConfig.builder().build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = algo.compute().centralities();
        assertThat(result.get(idFunction.of("a"))).isEqualTo(2 / 3.0);
        assertThat(result.get(idFunction.of("b"))).isEqualTo(1);
        assertThat(result.get(idFunction.of("c"))).isEqualTo(0);
        assertThat(result.get(idFunction.of("d"))).isEqualTo(2 / 3.0);
        assertThat(result.get(idFunction.of("e"))).isEqualTo(1);
        assertThat(result.get(idFunction.of("f"))).isEqualTo(0);
    }

    @Test
    void testCentralityWithWassermanFaust() {
        IdFunction idFunction = graph::toMappedNodeId;

        var algo = ClosenessCentrality.of(
            graph,
            ImmutableClosenessCentralityStreamConfig.builder().useWassermanFaust(true).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = algo.compute().centralities();
        assertThat(result.get(idFunction.of("a"))).isEqualTo(4 / 15.0);
        assertThat(result.get(idFunction.of("b"))).isEqualTo(2 / 5.0);
        assertThat(result.get(idFunction.of("c"))).isEqualTo(0);
        assertThat(result.get(idFunction.of("d"))).isEqualTo(4 / 15.0);
        assertThat(result.get(idFunction.of("e"))).isEqualTo(2 / 5.0);
        assertThat(result.get(idFunction.of("f"))).isEqualTo(0);
    }
}
