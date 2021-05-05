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
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiners;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.graphalgo.core.model.proto.TensorProto;
import org.neo4j.graphalgo.ml.model.proto.LinkPredictionProto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LinkPredictionSerializerTest {

    @ParameterizedTest
    @MethodSource("validLinkFeatureCombiners")
    void shouldSerializeLinkLogisticRegressionData(LinkFeatureCombiners linkFeatureCombiner) {

        var weightData = Matrix.fill(0.5D, 3, 4);
        var weight = new Weights<>(weightData);

        var modelData = LinkLogisticRegressionData.builder()
            .weights(weight)
            .linkFeatureCombiner(linkFeatureCombiner)
            .nodeFeatureDimension(10)
            .build();

        var serializer = new LinkPredictionSerializer();
        var serializedData = serializer.toSerializable(modelData);

        var serializedWeights = serializedData.getWeights();
        var serializedArray = serializedWeights.getDataList().stream().mapToDouble(Double::doubleValue).toArray();
        assertThat(serializedArray).containsExactly(weightData.data());

        assertThat(serializedWeights.getRows()).isEqualTo(weightData.rows());
        assertThat(serializedWeights.getCols()).isEqualTo(weightData.cols());

        assertEquals(linkFeatureCombiner.name(), serializedData.getLinkFeatureCombiner().name());
        assertEquals(10, serializedData.getNodeFeatureDimension());
    }

    @ParameterizedTest
    @MethodSource("validLinkFeatureCombiners")
    void shouldDeserializeLinkLogisticRegressionData(LinkFeatureCombiners linkFeatureCombiner) {
        var meh = new ArrayList<Double>(12);
        Collections.fill(meh, 0.5D);
        var serialized =
            LinkPredictionProto.LinkPredictionModelData.newBuilder()
                .setWeights(TensorProto.Matrix.newBuilder().addAllData(meh))
                .setLinkFeatureCombiner(LinkPredictionProto.LinkFeatureCombiner.valueOf(linkFeatureCombiner.name()))
                .setNodeFeatureDimension(10)
                .build();

        var serializer = new LinkPredictionSerializer();
        var deserializedModelData = serializer.deserializeModelData(serialized);

        assertThat(deserializedModelData).isNotNull();

        assertThat(deserializedModelData.weights().data().data()).containsExactly(meh
            .stream()
            .mapToDouble(Double::doubleValue)
            .toArray());
        assertEquals(linkFeatureCombiner, deserializedModelData.linkFeatureCombiner());
        assertEquals(10, deserializedModelData.nodeFeatureDimension());
    }

    private static Stream<Arguments> validLinkFeatureCombiners() {
        return Arrays.stream(LinkFeatureCombiners.values()).map(Arguments::of);
    }
}
