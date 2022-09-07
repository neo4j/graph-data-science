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
package org.neo4j.gds.ml.pipeline.node.regression.predict;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.InspectableTestProgressTracker;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.ml.pipeline.ImmutablePipelineGraphFilter;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.test.TestProc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.ml.pipeline.node.regression.predict.NodeRegressionModelTestUtil.createModelData;

class NodeRegressionPredictPipelineExecutorTest extends BaseProcTest {

    private static final String GRAPH_NAME = "g";

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

        graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "g").graphStore();
    }


    @Test
    void shouldPredict() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = NodeRegressionPredictPipelineBaseConfigImpl.builder()
                .modelUser("")
                .modelName("model")
                .graphName(GRAPH_NAME)
                .build();

            var pipeline = NodePropertyPredictPipeline.from(
                Stream.of(NodePropertyStepFactory.createNodePropertyStep("testProc", Map.of("mutateProperty", "community"))),
                Stream.of("community", "b", "c").map(NodeFeatureStep::of)
            );

            double[] weights = {2, -1, 3};
            var bias = 0.0;

            var expectedSchema = graphStore.schema();

            HugeDoubleArray predictions = new NodeRegressionPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                createModelData(weights, bias),
                ImmutablePipelineGraphFilter.builder()
                    .nodeLabels(List.of(NodeLabel.of("N")))
                    .relationshipTypes(List.of(RelationshipType.of("T")))
                    .build()
            ).compute();

            assertThat(graphStore.schema()).isEqualTo(expectedSchema);
            assertThat(predictions.toArray()).containsExactly(2.2, 4.0, 5.5, 6.2, 10.1);
        });
    }

    @Test
    void progressTracking() {
        var config = NodeRegressionPredictPipelineBaseConfigImpl.builder()
            .modelUser("")
            .modelName("model")
            .graphName(GRAPH_NAME)
            .build();

        var pipeline = NodePropertyPredictPipeline.from(
            Stream.of(NodePropertyStepFactory.createNodePropertyStep(
                "testProc",
                Map.of("mutateProperty", "prop")
            )),
            Stream.of(
                NodeFeatureStep.of("a"),
                NodeFeatureStep.of("b"),
                NodeFeatureStep.of("c"),
                NodeFeatureStep.of("prop")
            )
        );

        double[] weights = {1.0, 1.0, -2.0, -1.0};
        var bias = 3.0;
        var modelData = createModelData(weights, bias);

        var progressTracker = new InspectableTestProgressTracker(
            NodeRegressionPredictPipelineExecutor.progressTask("Node Regression Predict Pipeline", pipeline, graphStore),
            getUsername(),
            config.jobId()
        );

        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var pipelineExecutor = new NodeRegressionPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                progressTracker,
                modelData,
                ImmutablePipelineGraphFilter.builder()
                    .nodeLabels(List.of(NodeLabel.of("N")))
                    .relationshipTypes(List.of(RelationshipType.of("T")))
                    .build()
            );

            pipelineExecutor.compute();

            var expectedMessages = new ArrayList<>(List.of(
                "Node Regression Predict Pipeline :: Start",
                "Node Regression Predict Pipeline :: Execute node property steps :: Start",
                "Node Regression Predict Pipeline :: Execute node property steps :: TestAlgorithm :: Start",
                "Node Regression Predict Pipeline :: Execute node property steps :: TestAlgorithm 100%",
                "Node Regression Predict Pipeline :: Execute node property steps :: TestAlgorithm :: Finished",
                "Node Regression Predict Pipeline :: Execute node property steps :: Finished",
                "Node Regression Predict Pipeline :: Predict :: Start",
                "Node Regression Predict Pipeline :: Predict 100%",
                "Node Regression Predict Pipeline :: Predict :: Finished",
                "Node Regression Predict Pipeline :: Finished"
            ));

            assertThat(progressTracker.log().getMessages(INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(expectedMessages.toArray(String[]::new));
        });
        progressTracker.assertValidProgressEvolution();
    }

    @Test
    void failOnInvalidFeatureDimensions() {
        TestProcedureRunner.applyOnProcedure(db, TestProc.class, caller -> {
            var config = NodeRegressionPredictPipelineBaseConfigImpl.builder()
                .modelUser("")
                .modelName("model")
                .graphName(GRAPH_NAME)
                .build();

            var pipeline = NodePropertyPredictPipeline.from(
                Stream.of(),
                Stream.of("a").map(NodeFeatureStep::of)
            );

            double[] manyWeights = {-1.5, -2, 2.5, -1};
            var bias = 0.0;


            var pipelineExecutor = new NodeRegressionPredictPipelineExecutor(
                pipeline,
                config,
                caller.executionContext(),
                graphStore,
                ProgressTracker.NULL_TRACKER,
                createModelData(manyWeights, bias),
                ImmutablePipelineGraphFilter.builder()
                    .nodeLabels(List.of(NodeLabel.of("N")))
                    .relationshipTypes(List.of(RelationshipType.of("T")))
                    .build()
            );

            assertThatThrownBy(() -> pipelineExecutor.compute().toArray())
                .hasMessage("Model expected features ['a'] to have a total dimension of `4`, but got `1`.");
        });
    }
}
