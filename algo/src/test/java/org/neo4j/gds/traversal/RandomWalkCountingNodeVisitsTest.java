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
package org.neo4j.gds.traversal;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class RandomWalkCountingNodeVisitsTest {

    @GdlGraph
    private static final String GDL =
        "CREATE" +
            "  (a:Node1)" +
            ", (b:Node1)" +
            ", (c:Node2)" +
            ", (d:Isolated)" +
            ", (e:Isolated)" +
            ", (a)-[:REL1]->(b)" +
            ", (b)-[:REL1]->(a)" +
            ", (a)-[:REL1]->(c)" +
            ", (c)-[:REL2]->(a)" +
            ", (b)-[:REL2]->(c)" +
            ", (c)-[:REL2]->(b)";

    @Inject
    private TestGraph graph;

    @ParameterizedTest(name = "Concurrency: {0}")
    @ValueSource(ints = {1, 12})
    void randomWalksSansPaths(int concurrency, SoftAssertions assertions) {

        var randomWalks = RandomWalkCountingNodeVisits.create(
            graph,
            new Concurrency(concurrency),
            new WalkParameters(10, 80, 1.0, 1.0),
            List.of(),
            Optional.of(19L),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        );

        var result = randomWalks.compute();

        assertions.assertThat(result).isNotNull();
        assertions.assertThat(result.size()).isEqualTo(graph.nodeCount());
        assertions.assertThat(result.get(graph.toMappedNodeId("a"))).isEqualTo(545);
        assertions.assertThat(result.get(graph.toMappedNodeId("b"))).isEqualTo(789);
        assertions.assertThat(result.get(graph.toMappedNodeId("c"))).isEqualTo(1066);
        assertions.assertThat(result.get(graph.toMappedNodeId("d"))).isEqualTo(0);
        assertions.assertThat(result.get(graph.toMappedNodeId("e"))).isEqualTo(0);
    }
}
