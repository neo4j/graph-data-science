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
package org.neo4j.gds.ml.nodemodels.pipeline.predict;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestLog;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
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
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.nodemodels.logisticregression.ImmutableNodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationFeatureStep;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipeline;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineExecutor;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStep;
import org.neo4j.gds.test.TestProc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestLog.INFO;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@Neo4jModelCatalogExtension
class NodeClassificationPredictPipelineExecutorTest extends BaseProcTest {
    public static final String GRAPH_NAME = "g";

    @Neo4jGraph
    static String GDL = "CREATE " +
                        "  (n0:N {a: 1.0, b: 0.8, c: 1})" +
                        ", (n1:N {a: 2.0, b: 1.0, c: 1})" +
                        ", (n2:N {a: 3.0, b: 1.5, c: 1})" +
                        ", (n3:N {a: 0.0, b: 2.8, c: 1})" +
                        ", (n4:N {a: 1.0, b: 0.9, c: 1})" +
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
            GraphCreateProc.class,
            GraphStreamNodePropertiesProc.class
        );
        String createQuery = GdsCypher.call()
            .withNodeLabel("N")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("a", "b", "c"), DefaultValue.DEFAULT)
            .graphCreate(GRAPH_NAME)
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
            var config = new NodeClassificationPredictPipelineBaseConfigImpl(
                Optional.of(GRAPH_NAME),
                Optional.empty(),
                "",
                CypherMapWrapper.empty().withEntry("modelName", "model").withEntry("includePredictedProbabilities",true)
            );

            var pipeline = new NodeClassificationPipeline();
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("a"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("b"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("c"));

            var weights=new double[]{-2.0, -1.0, 3.0, 0.0,-1.5,-1.3,2.6,0.0};
            NodeLogisticRegressionData modelData = createModeldata(weights);

            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER,
                modelData
            );

            var predictionResult = pipelineExecutor.compute();

            assertThat(predictionResult.predictedClasses().size()).isEqualTo(graphStore.nodeCount());
            assertThat(predictionResult.predictedProbabilities()).isPresent();
            assertThat(predictionResult.predictedProbabilities().get().size()).isEqualTo(graphStore.nodeCount());

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @Test
    void shouldPredictWithNodePropertySteps() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = new NodeClassificationPredictPipelineBaseConfigImpl(
                Optional.of(GRAPH_NAME),
                Optional.empty(),
                "",
                CypherMapWrapper.empty().withEntry("modelName", "model").withEntry("includePredictedProbabilities",true)
            );

            var pipeline = new NodeClassificationPipeline();
            pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("a"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("b"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("c"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("degree"));
            var weights=new double[]{1.0,1.0,-2.0, -1.0, 3.0, 0.0,-1.5,-1.3,2.6,0.0};
            NodeLogisticRegressionData modelData = createModeldata(weights);

            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller,
                graphStore,
                GRAPH_NAME,
                ProgressTracker.NULL_TRACKER,
                modelData
            );

            var predictionResult = pipelineExecutor.compute();
            assertThat(predictionResult.predictedClasses().size()).isEqualTo(graphStore.nodeCount());
            assertThat(predictionResult.predictedProbabilities()).isPresent();
            assertThat(predictionResult.predictedProbabilities().get().size()).isEqualTo(graphStore.nodeCount());

            assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
            assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        });
    }

    @NotNull
    private NodeLogisticRegressionData createModeldata(double[] weights) {
        var idMap = new LocalIdMap();
        idMap.toMapped(0);
        idMap.toMapped(1);
        var modelData = ImmutableNodeLogisticRegressionData.of(
            new Weights<>(
                new Matrix(
                    weights,
                    2,
                    weights.length/2
                )),
            idMap
        );
        return modelData;
    }

    @Test
    void progressTracking() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = new NodeClassificationPredictPipelineBaseConfigImpl(
                Optional.of(GRAPH_NAME),
                Optional.empty(),
                "",
                CypherMapWrapper.empty().withEntry("modelName", "model").withEntry("includePredictedProbabilities",true)
            );

            var pipeline = new NodeClassificationPipeline();
            pipeline.addNodePropertyStep(NodePropertyStep.of("degree", Map.of("mutateProperty", "degree")));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("a"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("b"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("c"));
            pipeline.addFeatureStep(NodeClassificationFeatureStep.of("degree"));
            var weights=new double[]{1.0,1.0,-2.0, -1.0, 3.0, 0.0,-1.5,-1.3,2.6,0.0};
            NodeLogisticRegressionData modelData = createModeldata(weights);

            modelCatalog.set(Model.of(
                getUsername(),
                "model",
                NodeClassificationPipelineExecutor.MODEL_TYPE,
                GraphSchema.empty(),
                modelData,
                NodeClassificationPipelineTrainConfig.builder()
                    .modelName("model")
                    .pipeline("DUMMY")
                    .targetProperty("foo")
                    .build(),
                NodeClassificationPipelineModelInfo.builder()
                    .classes(modelData.classIdMap().originalIdsList())
                    .bestParameters(NodeLogisticRegressionTrainConfig.of(List.of("foo","bar"),"foo",Map.of()))
                    .metrics(Map.of())
                    .trainingPipeline(pipeline.copy())
                    .build()
            ));

            var log = new TestLog();
            var progressTracker = new TestProgressTracker(
                new NodeClassificationPredictPipelineAlgorithmFactory<>(
                    caller,
                    db.databaseId(),
                    modelCatalog
                ).progressTask(graphStore.getUnion(), config),
                log,
                1,
                EmptyTaskRegistryFactory.INSTANCE
            );

            var pipelineExecutor = new NodeClassificationPredictPipelineExecutor(
                pipeline,
                config,
                caller,
                graphStore,
                GRAPH_NAME,
                progressTracker,
                modelData
            );

            pipelineExecutor.compute();

            var expectedMessages = new ArrayList<>(List.of(
                "Node Classification Predict Pipeline :: Start",
                "Node Classification Predict Pipeline :: execute node property steps :: Start",
                "Node Classification Predict Pipeline :: execute node property steps :: step 1 of 1 :: Start",
                "Node Classification Predict Pipeline :: execute node property steps :: step 1 of 1 100%",
                "Node Classification Predict Pipeline :: execute node property steps :: step 1 of 1 :: Finished",
                "Node Classification Predict Pipeline :: execute node property steps :: Finished",
                "Node Classification Predict Pipeline :: Node classification predict :: Start",
                "Node Classification Predict Pipeline :: Node classification predict 100%",
                "Node Classification Predict Pipeline :: Node classification predict :: Finished",
                "Node Classification Predict Pipeline :: clean up graph store :: Start",
                "Node Classification Predict Pipeline :: clean up graph store 100%",
                "Node Classification Predict Pipeline :: clean up graph store :: Finished",
                "Node Classification Predict Pipeline :: Finished"
            ));

            assertThat(log.getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(expectedMessages.toArray(String[]::new));
        });
    }
}
