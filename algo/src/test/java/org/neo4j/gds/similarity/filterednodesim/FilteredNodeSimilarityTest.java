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
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.similarity.filtering.NodeFilterSpecFactory;

import java.util.List;

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
        var sourceNodeFilter = List.of(0L, 1L, 2L);

        var config = ImmutableFilteredNodeSimilarityStreamConfig.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .build();

        var nodeSimilarity = new FilteredNodeSimilarityFactory<>().build(
            graph,
            config,
            ProgressTracker.NULL_TRACKER
        );

        // no results for nodes that are not specified in the node filter -- nice
        var noOfResultsWithSourceNodeOutsideOfFilter = nodeSimilarity
            .computeToStream()
            .filter(res -> !sourceNodeFilter.contains(res.node1))
            .count();
        assertThat(noOfResultsWithSourceNodeOutsideOfFilter).isEqualTo(0L);

        // nodes outside of the node filter are not present as target nodes either -- not nice
        var noOfResultsWithTargetNodeOutSideOfFilter = nodeSimilarity
            .computeToStream()
            .filter(res -> !sourceNodeFilter.contains(res.node2))
            .count();
        assertThat(noOfResultsWithTargetNodeOutSideOfFilter).isGreaterThan(0);
    }

    @Test
    void shouldSurviveIoannisObjections() {
        var sourceNodeFilter = List.of(3L);

        var config = ImmutableFilteredNodeSimilarityStreamConfig.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .concurrency(1)
            .build();

        var nodeSimilarity = new FilteredNodeSimilarityFactory<>().build(
            graph,
            config,
            ProgressTracker.NULL_TRACKER
        );

        // no results for nodes that are not specified in the node filter -- nice
        var noOfResultsWithSourceNodeOutsideOfFilter = nodeSimilarity
            .computeToStream()
            .filter(res -> !sourceNodeFilter.contains(res.node1))
            .count();
        assertThat(noOfResultsWithSourceNodeOutsideOfFilter).isEqualTo(0L);

        // nodes outside of the node filter are not present as target nodes either -- not nice
        var noOfResultsWithTargetNodeOutSideOfFilter = nodeSimilarity
            .computeToStream()
            .filter(res -> !sourceNodeFilter.contains(res.node2))
            .count();
        assertThat(noOfResultsWithTargetNodeOutSideOfFilter).isGreaterThan(0);
    }

    @Test
    void shouldSurviveIoannisFurtherObjections() {
        var sourceNodeFilter = List.of(3L);

        var config = ImmutableFilteredNodeSimilarityStreamConfig.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .concurrency(1)
            .topK(0)
            .topN(10)
            .build();

        var nodeSimilarity = new FilteredNodeSimilarityFactory<>().build(
            graph,
            config,
            ProgressTracker.NULL_TRACKER
        );

        // no results for nodes that are not specified in the node filter -- nice
        var noOfResultsWithSourceNodeOutsideOfFilter = nodeSimilarity
            .computeToStream()
            .filter(res -> !sourceNodeFilter.contains(res.node1))
            .count();
        assertThat(noOfResultsWithSourceNodeOutsideOfFilter).isEqualTo(0L);

        // nodes outside of the node filter are not present as target nodes either -- not nice
        var noOfResultsWithTargetNodeOutSideOfFilter = nodeSimilarity
            .computeToStream()
            .filter(res -> !sourceNodeFilter.contains(res.node2))
            .count();
        assertThat(noOfResultsWithTargetNodeOutSideOfFilter).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void shouldLogProgressAccurately(int concurrency) {
        var sourceNodeFilter = List.of(2L, 3L);

        var config = ImmutableFilteredNodeSimilarityStreamConfig.builder()
            .sourceNodeFilter(NodeFilterSpecFactory.create(sourceNodeFilter))
            .concurrency(concurrency)
            .topK(0)
            .topN(10)
            .build();
        var progressTask = new FilteredNodeSimilarityFactory<>().progressTask(graph, config);
        TestLog log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            progressTask,
            log,
            concurrency,
            EmptyTaskRegistryFactory.INSTANCE
        );


        new FilteredNodeSimilarityFactory<>().build(
            graph,
            config,
            progressTracker
        ).compute();


        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "FilteredNodeSimilarity :: Start",
                "FilteredNodeSimilarity :: prepare :: Start",
                "FilteredNodeSimilarity :: prepare 33%",
                "FilteredNodeSimilarity :: prepare 55%",
                "FilteredNodeSimilarity :: prepare 66%",
                "FilteredNodeSimilarity :: prepare 100%",
                "FilteredNodeSimilarity :: prepare :: Finished",
                "FilteredNodeSimilarity :: compare node pairs :: Start",
                "FilteredNodeSimilarity :: compare node pairs 12%",
                "FilteredNodeSimilarity :: compare node pairs 25%",
                "FilteredNodeSimilarity :: compare node pairs 37%",
                "FilteredNodeSimilarity :: compare node pairs 50%",
                "FilteredNodeSimilarity :: compare node pairs 62%",
                "FilteredNodeSimilarity :: compare node pairs 75%",
                "FilteredNodeSimilarity :: compare node pairs 100%",
                "FilteredNodeSimilarity :: compare node pairs :: Finished",
                "FilteredNodeSimilarity :: Finished"
            );
    }
}
