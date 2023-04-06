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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.metrics.regression.RegressionMetrics;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStepTestUtil;
import org.neo4j.gds.ml.pipeline.PipelineTrainAlgorithmTest;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.gds.TestGdsVersion.testGdsVersion;

@GdlExtension
class NodeRegressionTrainAlgorithmTest {

    @GdlGraph
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

    @Inject
    private GraphStore graphStore;

    @TestFactory
    Stream<DynamicTest> baseTests() {
        var pipeline = new NodeRegressionTrainingPipeline();

        pipeline.nodePropertySteps().add(new ExecutableNodePropertyStepTestUtil.NodeIdPropertyStep(graphStore, "nodeId"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addFeatureStep(NodeFeatureStep.of("nodeId"));

        pipeline.addTrainerConfig(LinearRegressionTrainConfig.DEFAULT);

        RegressionMetrics evaluationMetric = RegressionMetrics.MEAN_ABSOLUTE_ERROR;
        var config = NodeRegressionPipelineTrainConfigImpl.builder()
            .pipeline("DUMMY_PIPE")
            .graphName("DUMMY_GRAPH")
            .modelUser("DUMMY_USER")
            .modelName("model")
            .targetProperty("t")
            .metrics(List.of(evaluationMetric))
            .build();


        var factory = new NodeRegressionTrainPipelineAlgorithmFactory(ExecutionContext.EMPTY, testGdsVersion);
        Supplier<NodeRegressionTrainAlgorithm> algoSupplier = () -> factory.build(
            graphStore,
            config,
            pipeline,
            ProgressTracker.NULL_TRACKER
        );

        return Stream.of(
            PipelineTrainAlgorithmTest.terminationFlagTest(algoSupplier.get()),
            PipelineTrainAlgorithmTest.trainsAModel(algoSupplier.get(), NodeRegressionTrainingPipeline.MODEL_TYPE),
            PipelineTrainAlgorithmTest.originalSchemaTest(algoSupplier.get(), pipeline),
            PipelineTrainAlgorithmTest.testParameterSpaceValidation(pipelineWithoutCandidate -> factory.build(
                graphStore,
                config,
                pipelineWithoutCandidate,
                ProgressTracker.NULL_TRACKER
            ), new NodeRegressionTrainingPipeline())
        );
    }

}
