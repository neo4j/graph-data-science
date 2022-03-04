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
package org.neo4j.gds.ml.linkmodels.pipeline.predict;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jModelCatalogExtension;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrain;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.test.TestProc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrain.MODEL_TYPE;

@Neo4jModelCatalogExtension
class LinkPredictionPredictPipelineExecutorTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";

    @Neo4jGraph
    static String GDL = "CREATE " +
                        "  (n0:N {a: 1.0, b: 0.8, c: 1.0})" +
                        ", (n1:N {a: 2.0, b: 1.0, c: 1.0})" +
                        ", (n2:N {a: 3.0, b: 1.5, c: 1.0})" +
                        ", (n3:N {a: 0.0, b: 2.8, c: 1.0})" +
                        ", (n4:N {a: 1.0, b: 0.9, c: 1.0})" +
                        ", (n1)-[:T]->(n2)" +
                        ", (n3)-[:T]->(n4)" +
                        ", (n1)-[:T]->(n3)" +
                        ", (n2)-[:T]->(n4)";

    private GraphStore graphStore;

    @Inject
    private ModelCatalog modelCatalog;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .yields();

        runQuery(createQuery);

        graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), "g").graphStore();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldPredict() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = LinkPredictionPredictPipelineStreamConfig.of(
                "",
                CypherMapWrapper.empty()
                    .withEntry("modelName", "model")
                    .withEntry("topN", 3)
                    .withEntry("graphName", GRAPH_NAME)
            );

            var pipeline = new LinkPredictionPipeline();
            pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c")));

            var modelData = ImmutableLogisticRegressionData.of(
                new Weights<>(
                    new Matrix(
                        new double[]{2.0, 1.0, -3.0},
                        1,
                        3
                    )),
                Optional.empty(),
                LinkPredictionTrain.makeClassIdMap()
            );

            var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
                pipeline,
                modelData,
                config,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            var predictionResult = pipelineExecutor.compute();


            var predictedLinks = predictionResult.stream().collect(Collectors.toList());
            assertThat(predictedLinks).hasSize(3);

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @Test
    void shouldPredictWithNodePropertySteps() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = LinkPredictionPredictPipelineStreamConfig.of(
                "",
                CypherMapWrapper.empty()
                    .withEntry("modelName", "model")
                    .withEntry("topN", 3)
                    .withEntry("graphName", GRAPH_NAME)
            );

            var pipeline = new LinkPredictionPipeline();
            pipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
                "degree",
                Map.of("mutateProperty", "degree")
            ));
            pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c", "degree")));

            var modelData = ImmutableLogisticRegressionData.of(
                new Weights<>(
                    new Matrix(
                        new double[]{2.0, 1.0, -3.0, -1.0},
                        1,
                        4
                    )),
                Optional.empty(),
                LinkPredictionTrain.makeClassIdMap()
            );

            var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
                pipeline,
                modelData,
                config,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER
            );

            var predictionResult = pipelineExecutor.compute();
            var predictedLinks = predictionResult.stream().collect(Collectors.toList());
            assertThat(predictedLinks).hasSize(3);

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @Test
    void progressTracking() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = LinkPredictionPredictPipelineStreamConfig.of(
                "",
                CypherMapWrapper.empty()
                    .withEntry("modelName", "model")
                    .withEntry("topN", 3)
                    .withEntry("graphName", GRAPH_NAME)
            );

            var pipeline = new LinkPredictionPipeline();
            pipeline.addNodePropertyStep(NodePropertyStepFactory.createNodePropertyStep(
                "degree",
                Map.of("mutateProperty", "degree")
            ));
            pipeline.addFeatureStep(new L2FeatureStep(List.of("a", "b", "c", "degree")));

            var modelData = ImmutableLogisticRegressionData.of(
                new Weights<>(
                    new Matrix(
                        new double[]{2.0, 1.0, -3.0, -1.0},
                        1,
                        4
                    )),
                Optional.empty(),
                LinkPredictionTrain.makeClassIdMap()
            );

            modelCatalog.set(Model.of(
                getUsername(),
                "model",
                MODEL_TYPE,
                GraphSchema.empty(),
                modelData,
                LinkPredictionTrainConfig.builder()
                    .modelName("model")
                    .pipeline("DUMMY")
                    .graphName(GRAPH_NAME)
                    .negativeClassWeight(1.0)
                    .build(),
                LinkPredictionModelInfo.of(LogisticRegressionTrainConfig.of(Map.of()), Map.of(), pipeline)
            ));

            var log = Neo4jProxy.testLog();
            var progressTracker = new TestProgressTracker(
                new LinkPredictionPredictPipelineAlgorithmFactory<>(caller.executionContext(), modelCatalog).progressTask(graphStore, config),
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );

            var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
                pipeline,
                modelData,
                config,
                caller.executionContext(),
                graphStore,
                GRAPH_NAME,
                progressTracker
            );

            pipelineExecutor.compute();

            var expectedMessages = new ArrayList<>(List.of(
                "Link Prediction Predict Pipeline :: Start",
                "Link Prediction Predict Pipeline :: execute node property steps :: Start",
                "Link Prediction Predict Pipeline :: execute node property steps :: step 1 of 1 :: Start",
                "Link Prediction Predict Pipeline :: execute node property steps :: step 1 of 1 100%",
                "Link Prediction Predict Pipeline :: execute node property steps :: step 1 of 1 :: Finished",
                "Link Prediction Predict Pipeline :: execute node property steps :: Finished",
                "Link Prediction Predict Pipeline :: exhaustive link prediction :: Start",
                "Link Prediction Predict Pipeline :: exhaustive link prediction 100%",
                "Link Prediction Predict Pipeline :: exhaustive link prediction :: Finished",
                "Link Prediction Predict Pipeline :: Finished"
            ));

            assertThat(log.getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(expectedMessages.toArray(String[]::new));
        });
    }
}
