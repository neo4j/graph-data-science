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
package org.neo4j.gds.ml.pipeline.nodePipeline;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfigImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfigImpl;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class NodeFeatureProducerTest {

    @GdlGraph(graphNamePrefix = "bipartite")
    private static final String DB_QUERY2 =
        "CREATE " +
        "  (x1:X {class: 0})" +
        ", (x2:X {class: 1})" +
        ", (x3:X {class: 0})" +
        ", (x4:X {class: 1})" +
        ", (x5:X {class: 0})" +
        ", (x6:X {class: 1})" +
        ", (x7:X {class: 0})" +
        ", (x8:X {class: 1})" +
        ", (x9:X {class: 0})" +

        ", (y1:Y {class: 2})" +

        ", (x2)-[:R]->(y1)" +
        ", (x4)-[:R]->(y1)" +
        ", (x6)-[:R]->(y1)" +
        ", (x8)-[:R]->(y1)";

    @Inject
    private GraphStore bipartiteGraphStore;

    private final static NodePropertyPredictionSplitConfig SPLIT_CONFIG = NodePropertyPredictionSplitConfigImpl
        .builder()
        .testFraction(0.33)
        .validationFolds(2)
        .build();

    @Test
    void shouldProduceCorrectNodeFeatures() {
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.setSplitConfig(SPLIT_CONFIG);
        pipeline.addFeatureStep(NodeFeatureStep.of("class"));

        var mlpTrainerConfig = MLPClassifierTrainConfigImpl.builder().hiddenLayerSizes(List.of(6,4)).build();

        pipeline.addTrainerConfig(mlpTrainerConfig);

        var metricSpecification = ClassificationMetricSpecification.Parser.parse("accuracy");
        var ncTrainConfig = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline("pipeline")
            .graphName("graphName")
            .modelUser("")
            .modelName("modelName")
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .targetNodeLabels(List.of("X"))
            .contextNodeLabels(List.of("Y"))
            .build();

        var ncFeatureProducer = NodeFeatureProducer.create(bipartiteGraphStore, ncTrainConfig, ExecutionContext.EMPTY, ProgressTracker.NULL_TRACKER);
        var ncFeatures = ncFeatureProducer.procedureFeatures(pipeline);

        assertThat(ncFeatures.size()).isEqualTo(9);

    }

}