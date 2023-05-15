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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.decisiontree.TreeNode;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.randomforest.ImmutableRandomForestClassifierData;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifier;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;
import org.neo4j.gds.nodeproperties.LongTestPropertyValues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;

@GdlExtension
class LinkPredictionPredictPipelineExecutorTest {

    @GdlGraph
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

    @Inject
    private GraphStore graphStore;


    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel = "(n0 :A {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n1: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n2: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n3: B {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n4: C {a: 1.0, b: 0.8, c: 1.0}), " +
                                  "(n5: A {a: 1.0, b: 0.8, c: 1.0})" +
                                  "(n0)-[:T]->(n1), " +
                                  "(n1)-[:T]->(n2), " +
                                  "(n2)-[:T]->(n0), " +
                                  "(n2)-[:T]->(n0), " +

                                  "(n0)-[:CONTEXT]->(n2)";

    @Inject
    GraphStore multiLabelGraphStore;
    private final String username = "user";

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldPredict() {
        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser(username)
            .modelName("model")
            .topN(3)
            .graphName("DUMMY")
            .build();

        var pipeline = LinkPredictionPredictPipeline.from(
            Stream.of(),
            Stream.of(new L2FeatureStep(List.of("a", "b", "c")))
        );

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    new double[]{2.0, 1.0, -3.0},
                    1,
                    3
                )),
            Weights.ofVector(0.0)
        );
        var progressTracker = new InspectableTestProgressTracker(
            LinkPredictionPredictPipelineExecutor.progressTask(
                "Link Prediction Train Pipeline",
                pipeline,
                graphStore,
                config
            ),
            "",
            config.jobId()
        );

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            pipeline,
            LogisticRegressionClassifier.from(modelData),
            ImmutableLPGraphStoreFilter.builder()
                .sourceNodeLabels(NodeLabel.listOf("N"))
                .targetNodeLabels(NodeLabel.listOf("N"))
                .nodePropertyStepsBaseLabels(List.of(NodeLabel.of("N")))
                .predictRelationshipTypes(RelationshipType.listOf("T"))
                .build(),
            config,
            ExecutionContext.EMPTY,
            graphStore,
            progressTracker
        );

        var predictionResult = pipelineExecutor.compute();


        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(3);

        assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
        assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
        progressTracker.assertValidProgressEvolution();
    }

    @Test
    void shouldPredictWithRandomForest() {
        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser(username)
            .modelName("model")
            .topN(3)
            .graphName("DUMMY")
            .build();

        var pipeline = LinkPredictionPredictPipeline.from(
            Stream.of(),
            Stream.of(new L2FeatureStep(List.of("a", "b", "c")))
        );

        var root = new TreeNode<>(0);
        var modelData = ImmutableRandomForestClassifierData
            .builder()
            .addDecisionTree(new DecisionTreePredictor<>(root))
            .featureDimension(3)
            .numberOfClasses(2)
            .build();

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            pipeline,
            new RandomForestClassifier(modelData),
            ImmutableLPGraphStoreFilter.builder()
                .sourceNodeLabels(NodeLabel.listOf("N"))
                .targetNodeLabels(NodeLabel.listOf("N"))
                .nodePropertyStepsBaseLabels(List.of(NodeLabel.of("N")))
                .predictRelationshipTypes(RelationshipType.listOf("T"))
                .build(),
            config,
            ExecutionContext.EMPTY,
            graphStore,
            ProgressTracker.NULL_TRACKER
        );

        var predictionResult = pipelineExecutor.compute();


        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(3);

        assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
        assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
    }

    @Test
    void shouldPredictWithNodePropertySteps() {
        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser(username)
            .modelName("model")
            .topN(3)
            .graphName("DUMMY")
            .build();

        var pipeline = LinkPredictionPredictPipeline.from(
            Stream.of(new NodeIdPropertyStep(graphStore, "degree")),
            Stream.of(new L2FeatureStep(List.of("a", "b", "c", "degree")))
        );

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    new double[]{2.0, 1.0, -3.0, -1.0},
                    1,
                    4
                )),
            Weights.ofVector(0.0)
        );

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            pipeline,
            LogisticRegressionClassifier.from(modelData),
            ImmutableLPGraphStoreFilter.builder()
                .sourceNodeLabels(NodeLabel.listOf("N"))
                .targetNodeLabels(NodeLabel.listOf("N"))
                .nodePropertyStepsBaseLabels(List.of(NodeLabel.of("N")))
                .predictRelationshipTypes(RelationshipType.listOf("T"))
                .build(),
            config,
            ExecutionContext.EMPTY,
            graphStore,
            ProgressTracker.NULL_TRACKER
        );

        var predictionResult = pipelineExecutor.compute();
        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize(3);

        assertThat(graphStore.relationshipTypes()).containsExactlyElementsOf(RelationshipType.listOf("T"));
        assertThat(graphStore.hasNodeProperty(graphStore.nodeLabels(), "degree")).isFalse();
    }

    @Test
    void shouldPredictFilteredWithNodePropertySteps() {
        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser("")
            .graphName("DUMMY")
            .modelName("model")
            .sampleRate(0.5)
            .topK(1)
            .build();

        LPGraphStoreFilter graphStoreFilter = ImmutableLPGraphStoreFilter.builder()
            .sourceNodeLabels(NodeLabel.listOf("A"))
            .targetNodeLabels(NodeLabel.listOf("B"))
            .nodePropertyStepsBaseLabels(NodeLabel.listOf("A", "B", "C"))
            .predictRelationshipTypes(RelationshipType.listOf("T"))
            .build();

        ExecutableNodePropertyStep nodePropertyStep = new TestFilteredNodePropertyStep(graphStoreFilter);

        var pipeline = LinkPredictionPredictPipeline.from(
            Stream.of(nodePropertyStep),
            Stream.of(new L2FeatureStep(List.of("a", "b", "c")))
        );

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    new double[]{2.0, 1.0, -3.0},
                    1,
                    3
                )),
            Weights.ofVector(0.0)
        );

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            pipeline,
            LogisticRegressionClassifier.from(modelData),
            graphStoreFilter,
            config,
            ExecutionContext.EMPTY,
            multiLabelGraphStore,
            ProgressTracker.NULL_TRACKER
        );

        var predictionResult = pipelineExecutor.compute();
        var predictedLinks = predictionResult.stream().collect(Collectors.toList());
        assertThat(predictedLinks).hasSize((int) multiLabelGraphStore
            .getGraph(graphStoreFilter.predictNodeLabels())
            .nodeCount());

        assertThat(multiLabelGraphStore.relationshipTypes()).containsExactlyInAnyOrderElementsOf(RelationshipType.listOf(
            "CONTEXT",
            "T"
        ));
    }

    @Test
    void progressTracking() {
        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser(username)
            .modelName("model")
            .topN(3)
            .graphName("DUMMY")
            .build();

        var pipeline = LinkPredictionPredictPipeline.from(
            Stream.of(new NodeIdPropertyStep(graphStore, "DegreeCentrality", "degree")),
            Stream.of(new L2FeatureStep(List.of("a", "b", "c", "degree")))
        );

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    new double[]{2.0, 1.0, -3.0, -1.0},
                    1,
                    4
                )),
            Weights.ofVector(0.0)
        );

        Model.of(
            MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            LinkPredictionTrainConfigImpl.builder()
                .modelUser(username)
                .modelName("model")
                .pipeline("DUMMY")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .targetRelationshipType("T")
                .graphName("DUMMY")
                .negativeClassWeight(1.0)
                .build(),
            LinkPredictionModelInfo.of(
                Map.of(),
                Map.of(),
                ModelCandidateStats.of(LogisticRegressionTrainConfig.DEFAULT, Map.of(), Map.of()),
                pipeline
            ));

        var progressTracker = new InspectableTestProgressTracker(
            LinkPredictionPredictPipelineExecutor.progressTask(
                "Link Prediction Predict Pipeline",
                pipeline,
                graphStore,
                config
            ),
            username,
            config.jobId()
        );

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            pipeline,
            LogisticRegressionClassifier.from(modelData),
            ImmutableLPGraphStoreFilter.builder()
                .sourceNodeLabels(NodeLabel.listOf("N"))
                .targetNodeLabels(NodeLabel.listOf("N"))
                .nodePropertyStepsBaseLabels(List.of(NodeLabel.of("N")))
                .predictRelationshipTypes(RelationshipType.listOf("T"))
                .build(),
            config,
            ExecutionContext.EMPTY,
            graphStore,
            progressTracker
        );

        pipelineExecutor.compute();

        var expectedMessages = new ArrayList<>(List.of(
            "Link Prediction Predict Pipeline :: Start",
            "Link Prediction Predict Pipeline :: Execute node property steps :: Start",
            "Link Prediction Predict Pipeline :: Execute node property steps :: DegreeCentrality :: Start",
            "Link Prediction Predict Pipeline :: Execute node property steps :: DegreeCentrality 100%",
            "Link Prediction Predict Pipeline :: Execute node property steps :: DegreeCentrality :: Finished",
            "Link Prediction Predict Pipeline :: Execute node property steps :: Finished",
            "Link Prediction Predict Pipeline :: Exhaustive link prediction :: Start",
            "Link Prediction Predict Pipeline :: Exhaustive link prediction 100%",
            "Link Prediction Predict Pipeline :: Exhaustive link prediction :: Finished",
            "Link Prediction Predict Pipeline :: Finished"
        ));

        assertThat(progressTracker.log().getMessages(INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(expectedMessages.toArray(String[]::new));
        progressTracker.assertValidProgressEvolution();
    }

    @Test
    void shouldEstimateMemoryWithLogisticRegression() {
        var pipeline = LinkPredictionPredictPipeline.EMPTY;
        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(
                new Matrix(
                    new double[]{2.0, 1.0, -3.0, -1.0},
                    1,
                    4
                )),
            Weights.ofVector(0.0)
        );

        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser(username)
            .modelName("model")
            .concurrency(1)
            .topN(10)
            .modelName("model")
            .modelUser("user")
            .graphName("DUMMY")
            .build();

        assertMemoryEstimation(
            () -> LinkPredictionPredictPipelineExecutor.estimate(new OpenModelCatalog(), pipeline, config, modelData),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            config.concurrency(),
            MemoryRange.of(433)
        );
    }

    @Test
    void shouldEstimateMemoryWithRandomForest() {
        var pipeline = LinkPredictionPredictPipeline.EMPTY;
        var root = new TreeNode<>(0);
        var modelData = ImmutableRandomForestClassifierData
            .builder()
            .addDecisionTree(new DecisionTreePredictor<>(root))
            .featureDimension(2)
            .numberOfClasses(2)
            .build();

        var config = LinkPredictionPredictPipelineStreamConfigImpl.builder()
            .modelUser(username)
            .modelName("model")
            .concurrency(1)
            .topN(10)
            .modelName("model")
            .graphName("DUMMY")
            .build();

        assertMemoryEstimation(
            () -> LinkPredictionPredictPipelineExecutor.estimate(new OpenModelCatalog(), pipeline, config, modelData),
            graphStore.nodeCount(),
            graphStore.relationshipCount(),
            config.concurrency(),
            MemoryRange.of(489)
        );
    }

    @Test
    void failOnInvalidFeatureDimension() {
        var tooManyFeatureWeights = new Weights<>(new Matrix(
            new double[]{2.0, 1.0, -3.0, 42, 42},
            1,
            5
        ));

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            tooManyFeatureWeights,
            Weights.ofVector(0.0)
        );

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            LinkPredictionPredictPipeline.from(
                Stream.of(),
                Stream.of(new L2FeatureStep(List.of("a", "b", "c")))
            ),
            LogisticRegressionClassifier.from(modelData),
            ImmutableLPGraphStoreFilter.builder()
                .sourceNodeLabels(NodeLabel.listOf("N"))
                .targetNodeLabels(NodeLabel.listOf("N"))
                .nodePropertyStepsBaseLabels(NodeLabel.listOf("N"))
                .predictRelationshipTypes(RelationshipType.listOf("T"))
                .build(),
            LinkPredictionPredictPipelineBaseConfigImpl.builder()
                .modelUser("")
                .modelName("model")
                .topN(3)
                .graphName("DUMMY")
                .build(),
            ExecutionContext.EMPTY,
            graphStore,
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(pipelineExecutor::compute)
            .hasMessageContaining("Model expected link features to have a total dimension of `5`, but got `3`. ")
            .hasMessageContaining(
                "This indicates the dimension of the node-properties ['a', 'b', 'c'] differ between the input and the original train graph.");
    }

    private static class TestFilteredNodePropertyStep implements ExecutableNodePropertyStep {
        private final LPGraphStoreFilter graphStoreFilter;

        TestFilteredNodePropertyStep(LPGraphStoreFilter graphStoreFilter) {
            this.graphStoreFilter = graphStoreFilter;
        }

        @Override
        public void execute(
            ExecutionContext executionContext,
            String graphName,
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relTypes,
            int trainConcurrency
        ) {
            assertThat(nodeLabels).containsExactlyInAnyOrderElementsOf(graphStoreFilter.nodePropertyStepsBaseLabels());
            assertThat(relTypes).containsExactlyInAnyOrderElementsOf(graphStoreFilter.predictRelationshipTypes());
        }

        @Override
        public Map<String, Object> config() {
            return Map.of(MUTATE_PROPERTY_KEY, "test");
        }

        @Override
        public String procName() {
            return "assert step filter";
        }

        @Override
        public MemoryEstimation estimate(
            ModelCatalog modelCatalog,
            String username,
            List<String> nodeLabels,
            List<String> relTypes
        ) {
            return MemoryEstimations.of("fake", MemoryRange.of(0));
        }

        @Override
        public String mutateNodeProperty() {
            return "test";
        }

        @Override
        public Map<String, Object> toMap() {
            return Map.of();
        }
    }

    private static class NodeIdPropertyStep implements ExecutableNodePropertyStep {
        private final GraphStore graphStore;
        private final String propertyName;
        private final String procName;

        NodeIdPropertyStep(GraphStore graphStore, String propertyName) {
            this(graphStore, "AddBogusNodePropertyStep", propertyName);
        }

        NodeIdPropertyStep(GraphStore graphStore, String procName, String propertyName) {
            this.graphStore = graphStore;
            this.propertyName = propertyName;
            this.procName = procName;
        }

        @Override
        public String procName() {
            return procName;
        }

        @Override
        public MemoryEstimation estimate(
            ModelCatalog modelCatalog,
            String username,
            List<String> nodeLabels,
            List<String> relTypes
        ) {
            throw new MemoryEstimationNotImplementedException();
        }

        @Override
        public String mutateNodeProperty() {
            return propertyName;
        }

        @Override
        public void execute(
            ExecutionContext executionContext,
            String graphName,
            Collection<NodeLabel> nodeLabels,
            Collection<RelationshipType> relTypes,
            int trainConcurrency
        ) {
            graphStore.addNodeProperty(
                graphStore.nodeLabels(),
                propertyName,
                new LongTestPropertyValues(nodeId -> nodeId)
            );
        }

        @Override
        public Map<String, Object> config() {
            return Map.of(MUTATE_PROPERTY_KEY, propertyName);
        }

        @Override
        public Map<String, Object> toMap() {
            return Map.of();
        }
    }
}
