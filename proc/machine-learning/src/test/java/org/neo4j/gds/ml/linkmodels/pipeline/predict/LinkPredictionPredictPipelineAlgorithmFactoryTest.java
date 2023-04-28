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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.model.InjectModelCatalog;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.ModelCatalogExtension;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.models.logisticregression.ImmutableLogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionModelInfo;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionPredictPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.L2FeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfigImpl;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline.MODEL_TYPE;

@ModelCatalogExtension
class LinkPredictionPredictPipelineAlgorithmFactoryTest {

    @InjectModelCatalog
    ModelCatalog modelCatalog;

    @Test
    void estimate() {
        withModelInCatalog();
        var factory = new LinkPredictionPredictPipelineAlgorithmFactory<>(ExecutionContext.EMPTY.withModelCatalog(modelCatalog));
        var config = LinkPredictionPredictPipelineStreamConfig.of(
            "testUser",
            CypherMapWrapper.create(
                Map.of(
                    "graphName", "g",
                    "modelName", "model",
                    "threshold", 0L,
                    "mutateRelationshipType", "PREDICTED",
                    "topN", 3
                )
            )
        );
        var estimate = factory
            .memoryEstimation(config)
            .estimate(GraphDimensions.of(10), 4);

        assertThat(estimate.memoryUsage().toString()).isEqualTo("548 Bytes");
    }

    private void withModelInCatalog() {
        var weights = new double[]{2.0, 1.0, -3.0};
        var pipeline = LinkPredictionPredictPipeline.from(Stream.of(), Stream.of(new L2FeatureStep(List.of("a", "b", "c"))));

        var modelData = ImmutableLogisticRegressionData.of(
            2,
            new Weights<>(new Matrix(
                weights,
                1,
                weights.length
            )),
            Weights.ofVector(0.0)
        );

        modelCatalog.set(Model.of(
            MODEL_TYPE,
            GraphSchema.empty(),
            modelData,
            LinkPredictionTrainConfigImpl.builder()
                .modelUser("testUser")
                .modelName("model")
                .pipeline("DUMMY")
                .sourceNodeLabel("N")
                .targetNodeLabel("N")
                .targetRelationshipType("T")
                .graphName("g")
                .negativeClassWeight(1.0)
                .build(),
            LinkPredictionModelInfo.of(
                Map.of(),
                Map.of(),
                ModelCandidateStats.of(LogisticRegressionTrainConfig.DEFAULT, Map.of(), Map.of()),
                pipeline
            )
        ));
    }
}
