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
package org.neo4j.gds.impl;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.impl.closeness.MSClosenessCentrality;

import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * (a)--(b) (d)--(e)
 *   \  /
 *   (c)
 *
 */
@GdlExtension
class ClosenessCentralityDiscoTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +

        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(c)" +

        ", (d)-[:TYPE]->(e)";

    @Inject
    private Graph graph;

    @Inject
    IdFunction idFunction;

    @Test
    void testHuge() {
        final MSClosenessCentrality algo = new MSClosenessCentrality(
            graph,
            2,
            true,
            AllocationTracker.empty(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        algo.compute();

        var actual = algo.resultStream().collect(Collectors.toMap(r -> r.nodeId, r -> r.centrality));

        var expected = Map.of(
            idFunction.of("a"), 0.5,
            idFunction.of("b"), 0.5,
            idFunction.of("c"), 0.5,
            idFunction.of("d"), 0.25,
            idFunction.of("e"), 0.25
        );

        assertThat(actual).isEqualTo(expected);
    }
}
