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
package org.neo4j.gds.ml.linkmodels;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiners;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionPredictor;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LinkPredictionEndToEndTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_QUERY =
        "  (a :N {bananas: 100.0, arrayProperty: [1.2, 1.2], a: 1.2, b: 1.2, t: 0})" +
        ", (b :N {bananas: 100.0, arrayProperty: [2.8, 2.5], a: 2.8, b: 2.5, t: 0})" +
        ", (c :N {bananas: 100.0, arrayProperty: [3.3, 0.5], a: 3.3, b: 0.5, t: 0})" +
        ", (d :N {bananas: 100.0, arrayProperty: [1.0, 0.5], a: 1.0, b: 0.5, t: 0})" +
        ", (e :N {bananas: 100.0, arrayProperty: [1.32, 0.5], a: 1.32, b: 0.5, t: 0})" +
        ", (f :N {bananas: 100.0, arrayProperty: [1.3, 1.5], a: 1.3, b: 1.5, t: 1})" +
        ", (g :N {bananas: 100.0, arrayProperty: [5.3, 10.5], a: 5.3, b: 10.5, t: 1})" +
        ", (h :N {bananas: 100.0, arrayProperty: [1.3, 2.5], a: 1.3, b: 2.5, t: 1})" +
        ", (i :N {bananas: 100.0, arrayProperty: [0.0, 66.8], a: 0.0, b: 66.8, t: 1})" +
        ", (j :N {bananas: 100.0, arrayProperty: [0.1, 2.8], a: 0.1, b: 2.8, t: 1})" +
        ", (k :N {bananas: 100.0, arrayProperty: [0.66, 2.8], a: 0.66, b: 2.8, t: 1})" +
        ", (l :N {bananas: 100.0, arrayProperty: [2.0, 10.8], a: 2.0, b: 10.8, t: 1})" +
        ", (m :N {bananas: 100.0, arrayProperty: [5.0, 7.8], a: 5.0, b: 7.8, t: 1})" +
        ", (n :N {bananas: 100.0, arrayProperty: [4.0, 5.8], a: 4.0, b: 5.8, t: 1})" +
        ", (o :N {bananas: 100.0, arrayProperty: [1.0, 0.9], a: 1.0, b: 0.9, t: 1})" +
        ", (a)-[:TRAIN { w: 1.0 }]->(b)" +
        ", (m)-[:TRAIN { w: 1.0 }]->(f)" +
        ", (h)-[:TRAIN { w: 0.0 }]->(n)" +
        ", (c)-[:TRAIN { w: 0.0 }]->(o)" +
        ", (c)-[:TEST { w: 1.0 }]->(d)" +
        ", (f)-[:TEST { w: 1.0 }]->(d)" +
        ", (i)-[:TEST { w: 0.0 }]->(l)" +
        ", (k)-[:TEST { w: 0.0 }]->(b)";

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @MethodSource("validLinkFeatureCombiners")
    void shouldProduceTheSamePredictionsAfterDeserialization(LinkFeatureCombiners linkFeatureCombiner) throws IOException {

        Map<String, Object> model2 = Map.of(
            "penalty", 1,
            "maxEpochs", 10000,
            "tolerance", 1e-5,
            "linkFeatureCombiner", linkFeatureCombiner.name()
        );

        List<String> featureProperties = List.of("bananas", "arrayProperty", "a", "b");
        var config = createConfig(List.of(model2), featureProperties);


        var lpTrain = new LinkPredictionTrain(graph, config, TestProgressLogger.NULL_LOGGER.getLog());

        var modelBeforeSerialization = lpTrain.compute();

        var linkPredictionPredict = predictAlgorithm(modelBeforeSerialization);

        var predictionsBeforeSerialization = linkPredictionPredict.compute();

        var serializer = new LinkPredictionSerializer();
        var serializableModel = serializer.toSerializable(modelBeforeSerialization.data());
        var modelMetaData = ModelMetaDataSerializer.toSerializable(modelBeforeSerialization);
        var deserializedModel = serializer.fromSerializable(serializableModel, modelMetaData);

        var nodeClassificationDeserializedPredict = predictAlgorithm(deserializedModel);

        var predictionsAfterSerialize = nodeClassificationDeserializedPredict.compute();

        assertThat(predictionsAfterSerialize)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(predictionsBeforeSerialization);
    }

    private LinkPredictionPredict predictAlgorithm(Model<LinkLogisticRegressionData, LinkPredictionTrainConfig> model) {
        var predictor = new LinkLogisticRegressionPredictor(model.data(), model.trainConfig().featureProperties());
        return new LinkPredictionPredict(
            predictor,
            graph,
            100,
            4,
            1000,
            AllocationTracker.empty(),
            ProgressLogger.NULL_LOGGER,
            0.0
        );
    }

    private LinkPredictionTrainConfig createConfig(
        Iterable<Map<String, Object>> modelCandidates,
        Iterable<String> featureProperties
    ) {
        return ImmutableLinkPredictionTrainConfig.builder()
            .modelName("model")
            .featureProperties(featureProperties)
            .validationFolds(2)
            .trainRelationshipType(RelationshipType.of("TRAIN"))
            .testRelationshipType(RelationshipType.of("TEST"))
            .concurrency(4)
            .randomSeed(19L)
            .params(modelCandidates)
            .negativeClassWeight(1.0)
            .build();
    }

    private static Stream<Arguments> validLinkFeatureCombiners() {
        return Arrays.stream(LinkFeatureCombiners.values()).map(Arguments::of);
    }
}
