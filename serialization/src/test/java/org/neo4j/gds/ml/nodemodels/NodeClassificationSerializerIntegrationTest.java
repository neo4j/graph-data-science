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
package org.neo4j.gds.ml.nodemodels;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.nodemodels.metrics.MetricSpecificationTest.allValidMetricSpecifications;

@GdlExtension
class NodeClassificationSerializerIntegrationTest {

    @GdlGraph(graphNamePrefix = "train")
    private static final String DB_QUERY =
        "  (:N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 1})" +
        ", (:N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 1})";

    @Inject
    TestGraph trainGraph;

    @ParameterizedTest
    @MethodSource("allValidMetricSpecificationsProxy")
    void roundTripTest(String metric) throws IOException {
        Map<String, Object> model2 = Map.of("penalty", 1, "maxEpochs", 10000, "tolerance", 1e-5);

        var log = new TestLog();
        var config = createConfig(List.of(model2), List.of("a", "b"), metric);

        var ncTrain = NodeClassificationTrain.create(trainGraph, config, AllocationTracker.empty(), ProgressTracker.NULL_TRACKER);

        var modelBeforeSerialization = ncTrain.compute();

        var serializer = new NodeClassificationSerializer();
        var serializableModel = serializer.toSerializable(modelBeforeSerialization.data());
        var modelMetaData = ModelMetaDataSerializer.toSerializable(modelBeforeSerialization);

        var deserializedModel = serializer.fromSerializable(serializableModel, modelMetaData);

        assertThat(deserializedModel)
            .usingRecursiveComparison()
            .ignoringFields(
                "trainConfig.params",
                "data.classIdMap",
                "stored"
            )
            .ignoringFieldsMatchingRegexes(
                "graphSchema\\.nodeSchema\\.properties\\..+\\.defaultValue\\.defaultValue"
            )
            .isEqualTo(modelBeforeSerialization);

    }

    private NodeClassificationTrainConfig createConfig(
        Iterable<Map<String, Object>> modelCandidates,
        Iterable<String> featureProperties,
        String metric
    ) {
        return ImmutableNodeClassificationTrainConfig.builder()
            .modelName("model")
            .featureProperties(featureProperties)
            .holdoutFraction(0.33)
            .validationFolds(2)
            .concurrency(4)
            .randomSeed(19L)
            .targetProperty("t")
            .metrics(List.of(MetricSpecification.parse(metric)))
            .params(modelCandidates)
            .build();
    }

    static List<String> allValidMetricSpecificationsProxy() {
        return allValidMetricSpecifications();
    }
}
