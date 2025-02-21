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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.utils.progress.PerDatabaseTaskStore;
import org.neo4j.gds.embeddings.graphsage.AggregatorType;
import org.neo4j.gds.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfigImpl;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainTask;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.termination.TerminationFlag;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.keepingFixedNumberOfDecimals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.embeddings.graphsage.GraphSageTestGraph.DUMMY_PROPERTY;

@GdlExtension
class NodeEmbeddingAlgorithmsGraphSageTest {
    @SuppressWarnings("unused")
    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @SuppressWarnings("unused")
    @Inject
    private Graph graph;

    @Test
    void testLogProgressForGraphSageTrain() {
        var config = GraphSageTrainConfigImpl.builder()
            .modelUser("DUMMY")
            .featureProperties(List.of(DUMMY_PROPERTY))
            .embeddingDimension(12)
            .aggregator(AggregatorType.POOL)
            .tolerance(1e-10)
            .sampleSizes(List.of(5, 3))
            .batchSize(4)
            .randomSeed(42L)
            .modelName("model")
            .relationshipWeightProperty("times")
            .epochs(2)
            .maxIterations(2)
            .build();

        var nodeEmbeddingAlgorithms = new NodeEmbeddingAlgorithms(null, null, TerminationFlag.RUNNING_TRUE);

        var log = new GdsTestLog();
        var progressTracker = new InspectableTestProgressTracker(
            GraphSageTrainTask.create(graph, config),
            config.username(),
            config.jobId(),
            new PerDatabaseTaskStore(Duration.ofMinutes(1)),
            new LoggerForProgressTrackingAdapter(log)
        );

        nodeEmbeddingAlgorithms.graphSageTrain(graph, config, progressTracker);

        assertThat(log.getMessages(INFO))
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .extracting(keepingFixedNumberOfDecimals(2))
            .containsExactly(
                "GraphSageTrain :: Start",
                "GraphSageTrain :: Prepare batches :: Start",
                "GraphSageTrain :: Prepare batches 20%",
                "GraphSageTrain :: Prepare batches 40%",
                "GraphSageTrain :: Prepare batches 60%",
                "GraphSageTrain :: Prepare batches 80%",
                "GraphSageTrain :: Prepare batches 100%",
                "GraphSageTrain :: Prepare batches :: Finished",
                "GraphSageTrain :: Train model :: Start",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Start",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 :: Start",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 25%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 50%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 75%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 100%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 :: Average loss per node: 26.52",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 1 of 2 :: Finished",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 :: Start",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 25%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 50%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 75%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 100%",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 :: Average loss per node: 22.35",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Iteration 2 of 2 :: Finished",
                "GraphSageTrain :: Train model :: Epoch 1 of 2 :: Finished",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Start",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 :: Start",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 25%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 50%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 75%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 100%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 :: Average loss per node: 22.43",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 1 of 2 :: Finished",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 :: Start",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 25%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 50%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 75%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 100%",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 :: Average loss per node: 25.86",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Iteration 2 of 2 :: Finished",
                "GraphSageTrain :: Train model :: Epoch 2 of 2 :: Finished",
                "GraphSageTrain :: Train model :: Finished",
                "GraphSageTrain :: Finished"
            );

        progressTracker.assertValidProgressEvolution();
    }
}
