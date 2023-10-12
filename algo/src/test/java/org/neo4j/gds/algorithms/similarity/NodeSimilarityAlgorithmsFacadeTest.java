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
package org.neo4j.gds.algorithms.similarity;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.nodesim.ImmutableNodeSimilarityStreamConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class NodeSimilarityAlgorithmsFacadeTest {

    @GdlGraph(orientation = Orientation.NATURAL, idOffset = 0)
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Person)" +
            ", (b:Person)" +
            ", (c:Person)" +
            ", (d:Person)" +
            ", (i1:Item)" +
            ", (i2:Item)" +
            ", (i3:Item)" +
            ", (i4:Item)" +
            ", (a)-[:LIKES {prop: 1.0}]->(i1)" +
            ", (a)-[:LIKES {prop: 1.0}]->(i2)" +
            ", (a)-[:LIKES {prop: 2.0}]->(i3)" +
            ", (b)-[:LIKES {prop: 1.0}]->(i1)" +
            ", (b)-[:LIKES {prop: 1.0}]->(i2)" +
            ", (c)-[:LIKES {prop: 1.0}]->(i3)" +
            ", (d)-[:LIKES {prop: 0.5}]->(i1)" +
            ", (d)-[:LIKES {prop: 1.0}]->(i2)" +
            ", (d)-[:LIKES {prop: 1.0}]->(i3)";

    @Inject
    private TestGraph graph;

    @Inject
    private GraphStore graphStore;

    private static final Collection<String> EXPECTED_OUTGOING = new HashSet<>();

    static {
        EXPECTED_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(0, 3, 1.0));
        EXPECTED_OUTGOING.add(resultString(1, 2, 0.0));
        EXPECTED_OUTGOING.add(resultString(1, 3, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 3, 1 / 3.0));
        // Add results in reverse direction because topK
        EXPECTED_OUTGOING.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(3, 0, 1.0));
        EXPECTED_OUTGOING.add(resultString(2, 1, 0.0));
        EXPECTED_OUTGOING.add(resultString(3, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(3, 2, 1 / 3.0));
    }

    @Test
    void shouldCompute() {
        var graphStoreCatalogServiceMock = mock(GraphStoreCatalogService.class);
        doReturn(Pair.of(graph, graphStore))
            .when(graphStoreCatalogServiceMock)
            .getGraphWithGraphStore(any(), any(), any(), any(), any());

        // mocking was getting increasingly hairy with the configuration... 🧔🏻
        var config = ImmutableNodeSimilarityStreamConfig
            .builder()
            .similarityCutoff(0.0)
            .concurrency(4)
            .build();

        var logMock = mock(Log.class);
        when(logMock.getNeo4jLog()).thenReturn(Neo4jProxy.testLog());

        var similarityAlgorithmsFacade = new SimilarityAlgorithmsFacade(
            new AlgorithmRunner(
                graphStoreCatalogServiceMock,
                mock(AlgorithmMemoryValidationService.class),
                TaskRegistryFactory.empty(),
                EmptyUserLogRegistryFactory.INSTANCE,
                logMock
            )
        );
        var nodeSimilarity = similarityAlgorithmsFacade.nodeSimilarity(
            "foo",
            config,
            null,
            null
        );

        assertThat(nodeSimilarity.result()).isPresent();
        Set<String> result = nodeSimilarity.result()
            .orElseThrow()
            .streamResult()
            .map(NodeSimilarityAlgorithmsFacadeTest::resultString)
            .collect(Collectors.toSet());

        assertThat(result)
            .containsExactlyElementsOf(EXPECTED_OUTGOING);
    }

    private static String resultString(SimilarityResult result) {
        return resultString(result.node1, result.node2, result.similarity);
    }

    private static String resultString(long node1, long node2, double similarity) {
        return formatWithLocale("%d,%d %f", node1, node2, similarity);
    }
}
