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
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.impl.closeness.MSClosenessCentrality;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.function.DoubleConsumer;

import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

    @Test
    void testHuge() {
        final MSClosenessCentrality algo = new MSClosenessCentrality(
            graph,
            AllocationTracker.empty(),
            2,
            Pools.DEFAULT,
            true
        );
        final DoubleConsumer mock = mock(DoubleConsumer.class);
        algo.compute();
        algo.resultStream()
            .forEach(r -> mock.accept(r.centrality));

        verify(mock, times(3)).accept(eq(0.5, 0.01));
        verify(mock, times(2)).accept(eq(0.25, 0.01));
    }
}
