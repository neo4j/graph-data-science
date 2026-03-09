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
package org.neo4j.gds.applications.algorithms.similarity;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.core.PlainSimpleRequestCorrelationId;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistry;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfigImpl;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class SimilarityAlgorithmsBusinessFacadeTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (a:Person)" +
            ", (b:Person)" +
            ", (i1:Item)" +
            ", (a)-[:LIKES {prop: 1.0}]->(i1)" +
            ", (b)-[:LIKES {prop: 1.0}]->(i1)";

    @Inject
    private TestGraph graph;


    @Test
    void shouldNotLogMessagesWhenLoggingIsDisabled() {
        var log = new GdsTestLog();
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .correlationId(PlainSimpleRequestCorrelationId.create())
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .terminationFlag(TerminationFlag.RUNNING_TRUE)
            .userLogRegistry(UserLogRegistry.EMPTY)
            .build();
        var progressTrackerCreator = new ProgressTrackerCreator(new LoggerForProgressTrackingAdapter(log), requestScopedDependencies);
        var similarityAlgorithms = new SimilarityAlgorithms(requestScopedDependencies.terminationFlag());

        var similarityBusiness = new SimilarityAlgorithmsBusinessFacade(similarityAlgorithms,progressTrackerCreator);

        similarityBusiness.nodeSimilarity(graph, NodeSimilarityBaseConfigImpl.builder().logProgress(false).build());

        assertThat(log.getMessages(INFO))
            .as("When progress logging is disabled we only log `start` and `finished`.")
            .extracting(removingThreadId())
            .containsExactly(
                "Node Similarity :: Start",
                "Node Similarity :: prepare :: Start",
                "Node Similarity :: prepare :: Finished",
                "Node Similarity :: compare node pairs :: Start",
                "Node Similarity :: compare node pairs :: Finished",
                "Node Similarity :: Finished"
            );
    }

}
