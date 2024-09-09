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
package org.neo4j.gds.closeness;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

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
    private TestGraph graph;

    @Test
    void testHuge() {
        IdFunction idFunction = graph::toMappedNodeId;

        var algo = new ClosenessCentrality(
            graph,
            new Concurrency(2),
            new WassermanFaustCentralityComputer(graph.nodeCount()),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var result = algo.compute().centralityScoreProvider();
        assertThat(result.applyAsDouble(idFunction.of("a"))).isCloseTo(0.5, Offset.offset(0.01));
        assertThat(result.applyAsDouble(idFunction.of("b"))).isCloseTo(0.5, Offset.offset(0.01));
        assertThat(result.applyAsDouble(idFunction.of("c"))).isCloseTo(0.5, Offset.offset(0.01));
        assertThat(result.applyAsDouble(idFunction.of("d"))).isCloseTo(0.25, Offset.offset(0.01));
        assertThat(result.applyAsDouble(idFunction.of("e"))).isCloseTo(0.25, Offset.offset(0.01));
    }
}
