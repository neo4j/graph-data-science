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

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ModelSerializer;
import org.neo4j.gds.embeddings.ddl4j.tensor.TensorSerializer;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkFeatureCombiners;
import org.neo4j.gds.ml.linkmodels.logisticregression.LinkLogisticRegressionData;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelMetaDataSerializer;
import org.neo4j.graphalgo.core.model.proto.ModelProto;
import org.neo4j.graphalgo.ml.model.proto.LinkPredictionProto;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class LinkPredictionSerializer implements ModelSerializer {

    @Override
    public GeneratedMessageV3 toSerializable(Object data) {
        LinkLogisticRegressionData modelData = (LinkLogisticRegressionData) data;
        var weightsMatrix = modelData.weights().data();
        var linkFeatureCombiner = modelData.linkFeatureCombiner();
        if (!(linkFeatureCombiner instanceof LinkFeatureCombiners)) {
            throw new IllegalStateException(formatWithLocale("LinkFeature combiner %s cannot be serialized.", linkFeatureCombiner.toString()));
        }
        var enumLinkFeatureCombiner = (LinkFeatureCombiners) linkFeatureCombiner;
        var serializableWeightsMatrix = TensorSerializer.toSerializable(weightsMatrix);
        return LinkPredictionProto.LinkPredictionModelData.newBuilder()
            .setWeights(serializableWeightsMatrix)
            .setLinkFeatureCombiner(LinkPredictionProto.LinkFeatureCombiner.valueOf(enumLinkFeatureCombiner.name()))
            .setNodeFeatureDimension(modelData.nodeFeatureDimension())
            .build();
    }

    @Override
    @NotNull
    public LinkLogisticRegressionData deserializeModelData(GeneratedMessageV3 generatedMessageV3) {
        var protoModel = (LinkPredictionProto.LinkPredictionModelData) generatedMessageV3;
        var weights = new Weights<>(TensorSerializer.fromSerializable(protoModel.getWeights()));
        var linkFeatureCombiner = LinkFeatureCombiners.valueOf(protoModel.getLinkFeatureCombiner().name());
        var nodeFeatureDimension = protoModel.getNodeFeatureDimension();

        return LinkLogisticRegressionData.builder()
            .weights(weights)
            .linkFeatureCombiner(linkFeatureCombiner)
            .nodeFeatureDimension(nodeFeatureDimension)
            .build();
    }

    @Override
    public Parser<LinkPredictionProto.LinkPredictionModelData> modelParser() {
        return LinkPredictionProto.LinkPredictionModelData.parser();
    }

    @TestOnly
    Model<LinkLogisticRegressionData, LinkPredictionTrainConfig> fromSerializable(
        GeneratedMessageV3 generatedMessageV3,
        ModelProto.ModelMetaData modelMetaData
    ) {
        var serializedData = (LinkPredictionProto.LinkPredictionModelData) generatedMessageV3;
        var weights = new Weights<>(TensorSerializer.fromSerializable(serializedData.getWeights()));

        return ModelMetaDataSerializer
            .<LinkLogisticRegressionData, LinkPredictionTrainConfig>fromSerializable(modelMetaData)
            .data(LinkLogisticRegressionData.builder()
                .weights(weights)
                .linkFeatureCombiner(LinkFeatureCombiners.valueOf(serializedData.getLinkFeatureCombiner().name()))
                .nodeFeatureDimension(serializedData.getNodeFeatureDimension())
                .build())
            .build();
    }
}
