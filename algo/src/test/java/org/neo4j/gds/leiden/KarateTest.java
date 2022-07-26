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
package org.neo4j.gds.leiden;

import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.beta.modularity.TestGraphs;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class KarateTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER = TestGraphs.KARATE_CLUB_GRAPH;
    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @ParameterizedTest
    @ValueSource(longs = {99999, 25, 323, 405, 58, 61, 7, 8123, 94, 19})
    void leiden(long seed) {
        var gamma = 1.0;
        Leiden leiden = new Leiden(graph, 5, gamma, 0.01, false, seed, 4, ProgressTracker.NULL_TRACKER);
        var leidenResult = leiden.compute();
        var communities = leidenResult.communities();
        var communitiesMap = LongStream
            .range(0, graph.nodeCount())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> communities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a1", "a2", "a3", "a4", "a8", "a10", "a12", "a13", "a14", "a18", "a20", "a22"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder(
                    "a9", "a15", "a16", "a19", "a21", "a23", "a27", "a30", "a31", "a33", "a34"
                ),
                community -> assertThat(community).containsExactlyInAnyOrder("a24", "a25", "a26", "a28", "a29", "a32"),
                community -> assertThat(community).containsExactlyInAnyOrder("a5", "a6", "a7", "a11", "a17"),
                community -> assertThat(community).containsExactlyInAnyOrder("a0")
            );
        assertThat(leidenResult.modularity()).isCloseTo(0.41880, Offset.offset(1e-3));

    }
}
