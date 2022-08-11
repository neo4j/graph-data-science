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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.assertj.core.util.DoubleComparator;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.ImmutableGraphSchema;
import org.neo4j.gds.api.schema.NodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.ModelCandidateStats;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.metrics.classification.ClassificationMetricSpecification;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionClassifier;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.training.TrainingStatistics;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationToModelConverterTest {

    private static final String PIPELINE_NAME = "magic flute";
    private static final String GRAPH_NAME = "el-grapho";
    private static final String USERNAME = "c3po";
    private static final String MODEL_NAME = "kardashian";

    @Test
    void convertsModel() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse("F1(class=1)");
        var classIdMap = LocalIdMap.of();
        var classCounts = new LongMultiSet();
        var metric = metricSpecification.createMetrics(classIdMap, classCounts).findFirst().orElseThrow();
        var data = LogisticRegressionData.standard(4, 42);
        var classifier = LogisticRegressionClassifier.from(data);
        var trainingStatistics = new TrainingStatistics(List.of(metric));
        var modelCandidate = LogisticRegressionTrainConfig.of(Map.of("penalty", 1, "maxEpochs", 1));

        trainingStatistics.addTestScore(metric, 0.799999);
        trainingStatistics.addOuterTrainScore(metric, 0.666666);
        trainingStatistics.addCandidateStats(ModelCandidateStats.of(modelCandidate,
            Map.of(metric, ModelStats.of(0.89999, 0.79999, 0.99999)),
            Map.of(metric, ModelStats.of(0.649999, 0.499999, 0.7999999))
        ));
        var ncResult = ImmutableNodeClassificationTrainResult.of(classifier, trainingStatistics, classIdMap, classCounts);
        var pipeline = new NodeClassificationTrainingPipeline();
        pipeline.nodePropertySteps().add(NodePropertyStepFactory.createNodePropertyStep(
            "testProc",
            Map.of("mutateProperty", "pr")
        ));
        pipeline.addFeatureStep(NodeFeatureStep.of("array"));
        pipeline.addFeatureStep(NodeFeatureStep.of("scalar"));
        pipeline.addFeatureStep(NodeFeatureStep.of("pr"));

        pipeline.addTrainerConfig(modelCandidate);
        var config = NodeClassificationPipelineTrainConfigImpl.builder()
            .pipeline(PIPELINE_NAME)
            .graphName(GRAPH_NAME)
            .modelUser(USERNAME)
            .modelName(MODEL_NAME)
            .concurrency(1)
            .randomSeed(42L)
            .targetProperty("t")
            .metrics(List.of(metricSpecification))
            .build();
        var converter = new NodeClassificationToModelConverter(pipeline, config);

        var originalSchema = ImmutableGraphSchema.builder()
            .nodeSchema(NodeSchema.builder().addLabel(NodeLabel.of("M")).build())
            .relationshipSchema(RelationshipSchema.builder().addRelationshipType(RelationshipType.of("R"), true).build())
            .putGraphProperty("array", PropertySchema.of("array", ValueType.DOUBLE_ARRAY))
            .putGraphProperty("scalar", PropertySchema.of("scalar", ValueType.DOUBLE))
            .build();
        var model = converter.toModel(ncResult, originalSchema).model();

        // test model meta data
        assertThat(model.creator()).isEqualTo(USERNAME);
        assertThat(model.algoType()).isEqualTo(NodeClassificationTrainingPipeline.MODEL_TYPE);
        assertThat(model.data()).isInstanceOf(LogisticRegressionData.class);
        assertThat(model.trainConfig()).isEqualTo(config);
        assertThat(model.graphSchema()).isEqualTo(originalSchema);
        assertThat(model.name()).isEqualTo(MODEL_NAME);
        assertThat(model.stored()).isFalse();
        assertThat(model.isPublished()).isFalse();
        assertThat(model.customInfo().bestParameters()).isEqualTo(modelCandidate);
        assertThat(model.customInfo().metrics().keySet()).containsExactly(metric.toString());
        assertThat(((Map<String, Object>) model.customInfo().metrics().get(metric.toString())).keySet())
            .containsExactlyInAnyOrder("train", "validation", "outerTrain", "test");

        // check metrics
        NodeClassificationPipelineModelInfo customInfo = model.customInfo();

        var expectedMetrics = Map.of(
            metric.toString(), Map.of(
              "test",  0.799999,
              "outerTrain", 0.666666,
              "validation", Map.of("avg", 0.649999, "max", 0.799999, "min", 0.499999),
              "train",   Map.of("avg", 0.89999, "max", 0.99999, "min", 0.79999)
            )
        );

        assertThat(customInfo.metrics())
            .usingRecursiveComparison()
            .withComparatorForType(new DoubleComparator(1e-5), Double.class)
            .isEqualTo(expectedMetrics);

        assertThat(customInfo.pipeline().nodePropertySteps()).isEqualTo(pipeline.nodePropertySteps());
        assertThat(customInfo.pipeline().featureProperties()).isEqualTo(pipeline.featureProperties());
    }
}
