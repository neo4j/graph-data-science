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

import org.neo4j.gds.core.ml.functions.Weights;
import org.neo4j.gds.embeddings.ddl4j.tensor.TensorSerializer;
import org.neo4j.graphalgo.core.model.proto.GraphSageCommonProto;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;

public final class MeanAggregatorSerializer {

    private MeanAggregatorSerializer() {}

    public static GraphSageProto.MeanAggregator toSerializable(MeanAggregator aggregator) {
        return GraphSageProto.MeanAggregator
            .newBuilder()
            .setWeights(TensorSerializer.toSerializable(aggregator.weightsData()))
            .setActivationFunction(GraphSageCommonProto.ActivationFunction.valueOf(aggregator.activationFunction().name()))
            .build();
    }

    public static MeanAggregator fromSerializable(GraphSageProto.MeanAggregator protoMeanAggregator) {
        var weights = new Weights<>(TensorSerializer.fromSerializable(protoMeanAggregator.getWeights()));
        var activationFunction = ActivationFunction
            .of(protoMeanAggregator.getActivationFunction().name());
        return new MeanAggregator(weights, activationFunction);
    }

}
