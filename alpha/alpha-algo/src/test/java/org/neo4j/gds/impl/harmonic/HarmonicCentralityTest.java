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
package org.neo4j.gds.impl.harmonic;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
public class HarmonicCentralityTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    public static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        
        ", (a)-[:TYPE]->(b)" +
        ", (b)-[:TYPE]->(c)" +
        ", (d)-[:TYPE]->(e)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldComputeHarmonicCentrality() {
        IdFunction mappedId = variable -> graph.toMappedNodeId(idFunction.of(variable));

        var harmonicCentrality = new HarmonicCentrality(
            graph,
            1,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = harmonicCentrality.compute();

        assertThat(result.getCentralityScore(mappedId.of("a"))).isEqualTo(0.375, within(0.1));
        assertThat(result.getCentralityScore(mappedId.of("b"))).isEqualTo(0.5, within(0.1));
        assertThat(result.getCentralityScore(mappedId.of("c"))).isEqualTo(0.375, within(0.1));
        assertThat(result.getCentralityScore(mappedId.of("d"))).isEqualTo(0.25, within(0.1));
        assertThat(result.getCentralityScore(mappedId.of("e"))).isEqualTo(0.25, within(0.1));
    }

    @Test
    void testLogging() {
        var task = Tasks.leaf("My task");
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(task, log, 1, EmptyTaskRegistryFactory.INSTANCE);

        var algo = new HarmonicCentrality(
            graph,
            1,
            Pools.DEFAULT,
            progressTracker
        );

        algo.compute();

        assertThat(log.getMessages(INFO))
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .containsExactly(
                "My task :: Start",
                "My task 100%",
                "My task :: Finished"
            );
    }
}
