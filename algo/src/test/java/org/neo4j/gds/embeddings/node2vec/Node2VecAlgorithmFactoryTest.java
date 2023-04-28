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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class Node2VecAlgorithmFactoryTest {

    @GdlGraph
    private static final String CYPHER =
        "CREATE" +
        "  (:N)" +
        ", (:N)" +
        ", (:N)" +
        ", (:N)" +
        ", (:N)";


    @Inject
    private Graph graph;

    @Test
    void shouldThrowIfRunningWouldOverflow() {

        var config = Node2VecStreamConfig.of(CypherMapWrapper.create(
            Map.of(
                "writeProperty", "embedding",
                "walksPerNode", Integer.MAX_VALUE,
                "walkLength", Integer.MAX_VALUE,
                "sudo", true
            )
        ));

        var factory = new Node2VecAlgorithmFactory<>();

        String expectedMessage = formatWithLocale(
            "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
            " Try reducing these parameters or run on a smaller graph.",
            graph.nodeCount(),
            Integer.MAX_VALUE,
            Integer.MAX_VALUE
        );

        assertThatIllegalArgumentException()
            .isThrownBy(() -> factory.build(graph, config, ProgressTracker.NULL_TRACKER))
            .withMessage(expectedMessage);
    }
}
