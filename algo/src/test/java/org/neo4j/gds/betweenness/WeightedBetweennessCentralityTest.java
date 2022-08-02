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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class WeightedBetweennessCentralityTest {

    // (a1)  (a2)
    //   \   /
    //    (b)
    //    / \
    //  (c) (d)
    //   \  /
    //    (e)
    //     |
    //    (f)
    @GdlGraph
    private static final String gdlGraphString =
        "CREATE " +
        "  (a1)-[:REL]->(b)" +
        ", (a2)-[:REL]->(b)" +
        ", (b) -[:REL]->(c)" +
        ", (b) -[:REL]->(d)" +
        ", (c) -[:REL]->(e)" +
        ", (d) -[:REL]->(e)" +
        ", (e) -[:REL]->(f)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void should() {
        var algo = new BetweennessCentrality(
            graph,
            new SelectionStrategy.RandomDegree(1),
            true,
            Pools.DEFAULT,
            8,
            ProgressTracker.NULL_TRACKER
        );
        var result = algo.compute();
        assertThat(result.get(idFunction.of("a1"))).isEqualTo(0.00000);
        assertThat(result.get(idFunction.of("a2"))).isEqualTo(0.00000);
        assertThat(result.get(idFunction.of("b"))).isEqualTo(0.00000);
        assertThat(result.get(idFunction.of("c"))).isEqualTo(0.00000);
        assertThat(result.get(idFunction.of("d"))).isEqualTo(0.00000);
        assertThat(result.get(idFunction.of("e"))).isEqualTo(0.00000);
        assertThat(result.get(idFunction.of("f"))).isEqualTo(0.00000);
    }
}
