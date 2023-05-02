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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

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

        var algo = ClosenessCentrality.of(
            graph,
            ImmutableClosenessCentralityStreamConfig.builder().concurrency(2).useWassermanFaust(true).build(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        
        var result = algo.compute().centralities();
        assertThat(result.get(idFunction.of("a"))).isCloseTo(0.5, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("b"))).isCloseTo(0.5, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("c"))).isCloseTo(0.5, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("d"))).isCloseTo(0.25, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("e"))).isCloseTo(0.25, Offset.offset(0.01));
    }
}
