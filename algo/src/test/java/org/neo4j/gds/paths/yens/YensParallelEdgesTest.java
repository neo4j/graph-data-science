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
package org.neo4j.gds.paths.yens;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.paths.yens.config.ImmutableShortestPathYensStreamConfig;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class YensParallelEdgesTest {

    static ImmutableShortestPathYensStreamConfig.Builder defaultSourceTargetConfigBuilder() {
        return ImmutableShortestPathYensStreamConfig.builder()
            .concurrency(1);
    }


    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c: Node)" +
        ", (d: Node)" +
        ", (a)-[:REL {cost: 1.0}]->(b)" +
        ", (a)-[:REL {cost: 2.0}]->(b)" +
        ", (a)-[:REL {cost: 3.0}]->(b)" +

        ", (a)-[:REL {cost: 200.0}]->(d)" +
        ", (b)-[:REL {cost: 1.0}]->(c)" +
        ", (b)-[:REL {cost: 2.0}]->(c)" +
        ", (b)-[:REL {cost: 3.0}]->(c)" +

        ", (c)-[:REL {cost: 1.0}]->(d)";


    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;


    @Test
    void shouldWorkWithParallelEdges() {
        var config = defaultSourceTargetConfigBuilder()
            .sourceNode(idFunction.of("a"))
            .targetNode(idFunction.of("d"))
            .k(9)
            .build();
        var yens = new YensFactory<>().build(graph, config, ProgressTracker.NULL_TRACKER);
        var result = yens.compute();
        var associatedCosts = result.pathSet().stream().mapToInt(path -> (int) path.totalCost()).toArray();
        assertThat(associatedCosts.length).isEqualTo(9);
        assertThat(associatedCosts).doesNotContain(200); //paths are  1 + (1..3)+(1..3)
    }
}
