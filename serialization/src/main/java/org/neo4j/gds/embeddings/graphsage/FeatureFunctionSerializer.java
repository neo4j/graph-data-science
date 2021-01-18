/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.ddl4j.tensor.TensorSerializer;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.model.proto.GraphSage;
import org.neo4j.graphalgo.core.model.proto.TensorProto;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class FeatureFunctionSerializer {

    private FeatureFunctionSerializer() {}

    public static GraphSage.FeatureFunction toSerializable(FeatureFunction featureFunction) {
        if (featureFunction instanceof SingleLabelFeatureFunction) {
            return GraphSage.FeatureFunction.newBuilder().setFunctionType(GraphSageProto.FeatureFunctionType.SINGLE).build();
        } else if(featureFunction instanceof MultiLabelFeatureFunction) {
            return GraphSage.FeatureFunction
                .newBuilder()
                .setFunctionType(GraphSageProto.FeatureFunctionType.MULTI)
                .putAllWeightsByLabel(unwrapWeightsByLabelMatrix(((MultiLabelFeatureFunction) featureFunction).weightsByLabel()))
                .build();
        }
        throw new IllegalArgumentException(formatWithLocale("Unknown feature function class: %s", featureFunction));
    }

    static FeatureFunction fromSerializable(GraphSage.FeatureFunction protoFeatureFunction, GraphSageTrainConfig config) throws
        IOException,
        ClassNotFoundException {
        switch (protoFeatureFunction.getFunctionType()) {
            case SINGLE:
                return new SingleLabelFeatureFunction();
            case MULTI:
                return new MultiLabelFeatureFunction(
                    wrapWeightsByLabelMatrix(protoFeatureFunction.getWeightsByLabelMap()),
                    config.projectedFeatureDimension().orElseThrow()
                );
            default:
                throw new IllegalArgumentException(formatWithLocale("Unknown proto feature function class: %s", protoFeatureFunction));
        }
    }

    private static Map<String, TensorProto.Matrix> unwrapWeightsByLabelMatrix(Map<NodeLabel, Weights<? extends Tensor<?>>> weightsByLabel) {
        return weightsByLabel.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().name(),
            entry -> TensorSerializer.toSerializable((Matrix) entry.getValue().data())
        ));
    }

    private static Map<NodeLabel, Weights<? extends Tensor<?>>> wrapWeightsByLabelMatrix(Map<String, TensorProto.Matrix> weightsByLabel) {
        return weightsByLabel.entrySet().stream().collect(Collectors.toMap(
            entry -> NodeLabel.of(entry.getKey()),
            entry -> new Weights<>(TensorSerializer.fromSerializable(entry.getValue()))
        ));
    }
}
