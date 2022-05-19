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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionData;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfigImpl;
import org.neo4j.gds.test.TestProc;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

class NodeRegressionTrainPipelineExecutorTest extends BaseProcTest {
    private static final String PIPELINE_NAME = "pipe";
    private static final String GRAPH_NAME = "g";

    @Neo4jGraph
    private static final String DB_QUERY =
        "CREATE " +
        "  (a1:N {scalar: 1.2, array: [1.0, -1.0], t: 0.5})" +
        ", (a2:N {scalar: 0.5, array: [1.0, -1.0], t: 4.5})" +
        ", (a3:N {scalar: 1.1, array: [1.0, -1.0], t: 0.5})" +
        ", (a4:N {scalar: 0.8, array: [1.0, -1.0], t: 9.5})" +
        ", (a5:N {scalar: 1.3, array: [1.0, -1.0], t: 1.5})" +
        ", (a6:N {scalar: 1.0, array: [2.0, -1.0], t: 10.5})" +
        ", (a7:N {scalar: 0.8, array: [2.0, -1.0], t: 1.5})" +
        ", (a8:N {scalar: 1.5, array: [2.0, -1.0], t: 11.5})" +
        ", (a9:N {scalar: 0.5, array: [2.0, -1.0], t: -1.5})" +
        ", (a1)-[:R]->(a2)" +
        ", (a1)-[:R]->(a4)" +
        ", (a3)-[:R]->(a5)" +
        ", (a5)-[:R]->(a8)" +
        ", (a4)-[:R]->(a6)" +
        ", (a4)-[:R]->(a9)" +
        ", (a2)-[:R]->(a8)";

    private GraphStore graphStore;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class);

        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("R")
            .withNodeProperties(List.of("array", "scalar", "t"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), GRAPH_NAME).graphStore();
    }

    @AfterEach
    void tearDown() {
        PipelineCatalog.removeAll();
    }

    @Test
    void trainsAModel() {
        var pipeline = new NodeRegressionTrainingPipeline();

        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.addFeatureStep(NodeFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addFeatureStep(NodeFeatureStep.of("pr"));

        LinearRegressionTrainConfig modelCandidate = LinearRegressionTrainConfig.DEFAULT;
        pipeline.addTrainerConfig(modelCandidate);

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        RegressionMetrics evaluationMetric = RegressionMetrics.MEAN_ABSOLUTE_ERROR;
        var config = NodeRegressionPipelineTrainConfigImpl.builder()
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .username(getUsername())
            .modelName("model")
            .concurrency(1)
            .randomSeed(1L)
            .targetProperty("t")
            .metrics(List.of(evaluationMetric))
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var result = new NodeRegressionTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER
            ).compute();

            var model = result.model();

            assertThat(model.algoType()).isEqualTo(NodeRegressionTrainingPipeline.MODEL_TYPE);
            assertThat(model.data()).isInstanceOf(LinearRegressionData.class);
            assertThat(model.trainConfig()).isEqualTo(config);
            assertThat(model.creator()).isEqualTo("");
            assertThat(model.graphSchema()).isEqualTo(graphStore.schema());
            assertThat(model.name()).isEqualTo("model");
            assertThat(model.stored()).isFalse();
            assertThat(model.customInfo().bestCandidate().trainerConfig().toMap()).isEqualTo(modelCandidate.toMap());
            assertThat(model.customInfo().outerTrainMetrics().keySet()).containsExactly(evaluationMetric);
            assertThat(model.customInfo().testMetrics().keySet()).containsExactly(evaluationMetric);
            assertThat(model.customInfo().bestCandidate().trainingStats().keySet()).containsExactly(evaluationMetric);
            assertThat(model.customInfo().bestCandidate().validationStats().keySet()).containsExactly(evaluationMetric);
        });
    }

    @Test
    void shouldLogProgress() {
        var pipeline = new NodeRegressionTrainingPipeline();

        pipeline.addFeatureStep(NodeFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addTrainerConfig(LinearRegressionTrainConfig.DEFAULT);

        pipeline.setSplitConfig(NodePropertyPredictionSplitConfigImpl.builder()
            .testFraction(0.3)
            .validationFolds(2)
            .build()
        );

        var config = NodeRegressionPipelineTrainConfigImpl.builder()
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .username(getUsername())
            .modelName("model")
            .concurrency(1)
            .randomSeed(1L)
            .targetProperty("t")
            .metrics(List.of(RegressionMetrics.MEAN_ABSOLUTE_ERROR))
            .build();

        var log = Neo4jProxy.testLog();
        var taskStore = new GlobalTaskStore();
        var progressTracker = new InspectableTestProgressTracker(
            NodeRegressionTrainPipelineExecutor.progressTask(pipeline, graphStore.nodeCount()),
            log,
            1,
            getUsername(),
            config.jobId(),
            taskStore
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            new NodeRegressionTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                progressTracker
            ).compute();

            assertThat(log.getMessages(TestLog.WARN))
                .extracting(removingThreadId())
                .containsExactly(
                    "Node Regression Train Pipeline :: The specified `testFraction` leads to a very small test set with only 3 node(s). " +
                    "Proceeding with such a small set might lead to unreliable results.",
                    "Node Regression Train Pipeline :: The specified `validationFolds` leads to very small validation sets with only 3 node(s). " +
                    "Proceeding with such small sets might lead to unreliable results."
                );

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .contains(
                    "Node Regression Train Pipeline :: Train set size is 6",
                    "Node Regression Train Pipeline :: Test set size is 3"
                );
        });
        progressTracker.assertValidProgressEvolution();
    }

    @Test
    void failsOnInvalidTargetProperty() {
        var pipeline = new NodeRegressionTrainingPipeline();
        pipeline.featureProperties().add("array");

        var config = NodeRegressionPipelineTrainConfigImpl.builder()
            .username(getUsername())
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .modelName("myModel")
            .targetProperty("INVALID_PROPERTY")
            .metrics(List.of(RegressionMetrics.ROOT_MEAN_SQUARED_ERROR))
            .build();

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var ncPipeTrain = new NodeRegressionTrainPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER
            );
            assertThatThrownBy(ncPipeTrain::compute)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Target property `INVALID_PROPERTY` not found in graph with node properties:");
        });
    }
}
