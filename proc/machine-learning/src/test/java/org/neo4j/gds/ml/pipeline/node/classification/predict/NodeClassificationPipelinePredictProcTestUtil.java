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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.junit.jupiter.params.provider.Arguments;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfigImpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class NodeClassificationPipelinePredictProcTestUtil {

    static final String GRAPH_NAME = "g";

    static final String TEST_GRAPH_QUERY =
        "CREATE " +
        " (n1:N {a: -1.36753705, b:  1.46853155})" +
        ", (n2:N {a: -1.45431768, b: -1.67820474})" +
        ", (n3:N {a: -0.34216825, b: -1.31498086})" +
        ", (n4:N {a: -0.60765016, b:  1.0186564})" +
        ", (n5:N {a: -0.48403364, b: -0.49152604})";

    private NodeClassificationPipelinePredictProcTestUtil() {}

    static void addPipelineModelWithFeatures(
        ModelCatalog modelCatalog,
        String graphName,
        String username,
        int dimensionOfNodeFeatures
    ) {
        addPipelineModelWithFeatures(modelCatalog, graphName, username, dimensionOfNodeFeatures, List.of("a", "b"));
    }

    static Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> createModel(
        String graphName,
        String username,
        int dimensionOfNodeFeatures,
        List<String> nodeFeatures
    ) {
        var pipeline = NodePropertyPredictPipeline.from(
            Stream.of(NodePropertyStepFactory.createNodePropertyStep(
                "degree",
                Map.of("mutateProperty", "degree")
            )),
            Stream.concat(nodeFeatures.stream(), Stream.of("degree"))
                .map(NodeFeatureStep::of)
        );

        var weights = Matrix.create(0.0, 2, dimensionOfNodeFeatures + 1);
        var bias = Vector.create(0, 2);
        var value = new MutableDouble();
        for (int row = 0; row < weights.rows(); row++) {
            for (int col = 0; col < weights.cols() + 1; col++) {
                if (col == weights.cols()) {
                    bias.setDataAt(row, value.getAndIncrement());
                } else {
                    weights.setDataAt(row, col, value.getAndIncrement());
                }
            }
        }

        var modelData = createClassifierData(weights.data(), bias.data());
        return Model.of(
            NodeClassificationTrainingPipeline.MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            NodeClassificationPipelineTrainConfigImpl.builder()
                .modelUser(username)
                .modelName("model")
                .graphName(graphName)
                .pipeline("DUMMY")
                .metrics(ClassificationMetricSpecification.Parser.parse(List.of("F1_MACRO")))
                .targetProperty("foo")
                .build(),
            NodeClassificationPipelineModelInfo.of(
                Map.of(),
                Map.of(),
                ModelCandidateStats.of(LogisticRegressionTrainConfig.DEFAULT, Map.of(), Map.of()),
                pipeline,
                List.of(0L, 1L)
            )
        );
    }

    static void addPipelineModelWithFeatures(
        ModelCatalog modelCatalog,
        String graphName,
        String username,
        int dimensionOfNodeFeatures,
        List<String> nodeFeatures
    ) {
        modelCatalog.set(createModel(graphName, username, dimensionOfNodeFeatures, nodeFeatures));
    }

    static LogisticRegressionData createClassifierData(double[] weights, double[] bias) {
        return ImmutableLogisticRegressionData.builder()
            .weights(new Weights<>(
                new Matrix(
                    weights,
                    2,
                    weights.length / 2
                ))
            )
            .bias(new Weights<>(new Vector(bias)))
            .numberOfClasses(2)
            .build();
    }

    static Stream<Arguments> graphNameOrConfigurations() {
        MemoryRange pipelineExecutorEstimation = MemoryRange.of(752L);

        return Stream.of(
            Arguments.of("g", pipelineExecutorEstimation),
            Arguments.of(
                Map.of("nodeProjection", "*", "relationshipProjection", "*"),
                MemoryRange.of(296_072)
            )
        );
    }
}
