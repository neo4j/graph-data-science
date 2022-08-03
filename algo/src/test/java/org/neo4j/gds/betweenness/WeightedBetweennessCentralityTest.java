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

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Optional;

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
        "  (a1)-[:REL {weight: 1.0}]->(b)" +
        ", (a2)-[:REL {weight: 1.0}]->(b)" +
        ", (b) -[:REL {weight: 1.0}]->(c)" +
        ", (b) -[:REL {weight: 1.0}]->(d)" +
        ", (c) -[:REL {weight: 1.0}]->(e)" +
        ", (d) -[:REL {weight: 1.0}]->(e)" +
        ", (e) -[:REL {weight: 1.0}]->(f)";

    @Inject
    private Graph graph;

    @Test
    void shouldEqualWithUnweightedWhenWeightsAreEqual() {
        var algoWeighted = new BetweennessCentrality(
            graph,
            new SelectionStrategy.RandomDegree(7, Optional.of(42L)),
            true,
            Pools.DEFAULT,
            8,
            ProgressTracker.NULL_TRACKER
        );
        var algoUnweighted = new BetweennessCentrality(
            graph,
            new SelectionStrategy.RandomDegree(7, Optional.of(42L)),
            false,
            Pools.DEFAULT,
            8,
            ProgressTracker.NULL_TRACKER
        );
        var resultWeighted = algoWeighted.compute();
        var resultUnweighted = algoUnweighted.compute();

        SoftAssertions softAssertions = new SoftAssertions();
        graph.forEachNode(nodeId -> {
                softAssertions.assertThat(resultWeighted.get(nodeId)).isEqualTo(resultUnweighted.get(nodeId));
                return true;
            }
        );

        softAssertions.assertAll();
    }
}
