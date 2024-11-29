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
package org.neo4j.gds.similarity.filterednodesim;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithms;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class FilteredNodeSimilarityTest {

    @GdlGraph
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

    @Test
    void should() {
        var similarityAlgorithms = new SimilarityAlgorithms(null, TerminationFlag.RUNNING_TRUE);

        var sourceNodeFilter = Stream.of("a", "b", "c").map(graph::toOriginalNodeId).collect(Collectors.toList());

        var config = FilteredNodeSimilarityStreamConfigImpl.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .build();

        // no results for nodes that are not specified in the node filter -- nice
        var noOfResultsWithSourceNodeOutsideOfFilter = similarityAlgorithms.filteredNodeSimilarity(graph, config, ProgressTracker.NULL_TRACKER)
            .streamResult()
            .filter(res -> !sourceNodeFilter.contains(graph.toOriginalNodeId(res.node1)))
            .count();
        assertThat(noOfResultsWithSourceNodeOutsideOfFilter).isEqualTo(0L);

        // nodes outside of the node filter are not present as target nodes either -- not nice
        var noOfResultsWithTargetNodeOutSideOfFilter = similarityAlgorithms.filteredNodeSimilarity(graph, config, ProgressTracker.NULL_TRACKER)
            .streamResult()
            .filter(res -> !sourceNodeFilter.contains(graph.toOriginalNodeId(res.node2)))
            .count();
        assertThat(noOfResultsWithTargetNodeOutSideOfFilter).isGreaterThan(0);
    }

    @Test
    void shouldSurviveIoannisObjections() {
        var similarityAlgorithms = new SimilarityAlgorithms(null, TerminationFlag.RUNNING_TRUE);

        var sourceNodeFilter = List.of(graph.toOriginalNodeId("d"));

        var config = FilteredNodeSimilarityStreamConfigImpl.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .concurrency(1)
            .build();

        // no results for nodes that are not specified in the node filter -- nice
        var noOfResultsWithSourceNodeOutsideOfFilter = similarityAlgorithms.filteredNodeSimilarity(graph, config, ProgressTracker.NULL_TRACKER)
            .streamResult()
            .filter(res -> !sourceNodeFilter.contains(graph.toOriginalNodeId(res.node1)))
            .count();
        assertThat(noOfResultsWithSourceNodeOutsideOfFilter).isEqualTo(0L);

        // nodes outside of the node filter are not present as target nodes either -- not nice
        var noOfResultsWithTargetNodeOutSideOfFilter = similarityAlgorithms.filteredNodeSimilarity(graph, config, ProgressTracker.NULL_TRACKER)
            .streamResult()
            .filter(res -> !sourceNodeFilter.contains(graph.toOriginalNodeId(res.node2)))
            .count();
        assertThat(noOfResultsWithTargetNodeOutSideOfFilter).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldSurviveIoannisFurtherObjections(boolean enableWcc) {
        var similarityAlgorithms = new SimilarityAlgorithms(null, TerminationFlag.RUNNING_TRUE);

        var sourceNodeFilter = List.of(graph.toOriginalNodeId("d"));

        var config = FilteredNodeSimilarityStreamConfigImpl.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .concurrency(1)
            .useComponents(enableWcc)
            .topK(1)
            .topN(10)
            .build();

        // no results for nodes that are not specified in the node filter -- nice
        var noOfResultsWithSourceNodeOutsideOfFilter = similarityAlgorithms.filteredNodeSimilarity(graph, config, ProgressTracker.NULL_TRACKER)
            .streamResult()
            .filter(res -> !sourceNodeFilter.contains(graph.toOriginalNodeId(res.node1)))
            .count();
        assertThat(noOfResultsWithSourceNodeOutsideOfFilter).isEqualTo(0L);

        // nodes outside of the node filter are not present as target nodes either -- not nice
        var noOfResultsWithTargetNodeOutSideOfFilter = similarityAlgorithms.filteredNodeSimilarity(graph, config, ProgressTracker.NULL_TRACKER)
            .streamResult()
            .filter(res -> !sourceNodeFilter.contains(graph.toOriginalNodeId(res.node2)))
            .count();
        assertThat(noOfResultsWithTargetNodeOutSideOfFilter).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void shouldLogProgressAccurately(int concurrencyValue) {
        var log = new GdsTestLog();
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .terminationFlag(TerminationFlag.RUNNING_TRUE)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .build();
        var progressTrackerCreator = new ProgressTrackerCreator(log, requestScopedDependencies);
        var similarityAlgorithms = new SimilarityAlgorithms(progressTrackerCreator, requestScopedDependencies.terminationFlag());

        var sourceNodeFilter = List.of(graph.toOriginalNodeId("c"), graph.toOriginalNodeId("d"));
        var concurrency = new Concurrency(concurrencyValue);
        var config = FilteredNodeSimilarityStreamConfigImpl.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .concurrency(concurrency.value())
            .topK(1)
            .topN(10)
            .build();
        similarityAlgorithms.filteredNodeSimilarity(graph, config);

        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "Filtered Node Similarity :: Start",
                "Filtered Node Similarity :: prepare :: Start",
                "Filtered Node Similarity :: prepare 33%",
                "Filtered Node Similarity :: prepare 55%",
                "Filtered Node Similarity :: prepare 66%",
                "Filtered Node Similarity :: prepare 100%",
                "Filtered Node Similarity :: prepare :: Finished",
                "Filtered Node Similarity :: compare node pairs :: Start",
                "Filtered Node Similarity :: compare node pairs 12%",
                "Filtered Node Similarity :: compare node pairs 25%",
                "Filtered Node Similarity :: compare node pairs 37%",
                "Filtered Node Similarity :: compare node pairs 50%",
                "Filtered Node Similarity :: compare node pairs 62%",
                "Filtered Node Similarity :: compare node pairs 75%",
                "Filtered Node Similarity :: compare node pairs 100%",
                "Filtered Node Similarity :: compare node pairs :: Finished",
                "Filtered Node Similarity :: Finished"
            );
    }
}
