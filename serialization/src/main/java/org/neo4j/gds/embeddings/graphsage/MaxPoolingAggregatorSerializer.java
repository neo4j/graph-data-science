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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.embeddings.ddl4j.tensor.TensorSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSageCommonProto;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;

public final class MaxPoolingAggregatorSerializer {

    private MaxPoolingAggregatorSerializer() {}

    public static GraphSageProto.MaxPoolingAggregator toSerializable(
        MaxPoolingAggregator aggregator
    ) {
        return GraphSageProto.MaxPoolingAggregator
            .newBuilder()
            .setPoolWeights(TensorSerializer.toSerializable(aggregator.poolWeights()))
            .setSelfWeights(TensorSerializer.toSerializable(aggregator.selfWeights()))
            .setNeighborsWeights(TensorSerializer.toSerializable(aggregator.neighborsWeights()))
            .setBias(TensorSerializer.toSerializable(aggregator.bias()))
            .setActivationFunction(GraphSageCommonProto.ActivationFunction.valueOf(aggregator.activationFunction().name()))
            .build();
    }

    public static MaxPoolingAggregator fromSerializable(GraphSageProto.MaxPoolingAggregator protoMaxPoolingAggregator) {
        var poolWeights = new Weights<>(TensorSerializer.fromSerializable(protoMaxPoolingAggregator.getPoolWeights()));
        var selfWeights = new Weights<>(TensorSerializer.fromSerializable(protoMaxPoolingAggregator.getSelfWeights()));
        var neighborsWeights = new Weights<>(TensorSerializer.fromSerializable(protoMaxPoolingAggregator.getNeighborsWeights()));
        var bias = new Weights<>(TensorSerializer.fromSerializable(protoMaxPoolingAggregator.getBias()));
        var activationFunction = ActivationFunction
            .of(protoMaxPoolingAggregator.getActivationFunction().name());

        return new MaxPoolingAggregator(poolWeights, selfWeights, neighborsWeights, bias, activationFunction);
    }

}
