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
import org.neo4j.gds.core.model.proto.GraphSageProto;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LayerSerializer {

    private LayerSerializer() {}

    public static Layer[] fromSerializable(List<GraphSageProto.Layer> layers) {
        return layers.stream()
            .map(LayerSerializer::fromSerializable)
            .collect(Collectors.toList())
            .toArray(new Layer[]{});
    }

    public static GraphSageProto.Layer toSerializable(Layer layer) {
        var builder = GraphSageProto.Layer.newBuilder();
        var aggregator = layer.aggregator();
        switch (aggregator.type()) {
            case MEAN:
                builder.setMean(MeanAggregatorSerializer.toSerializable((MeanAggregator) aggregator));
                break;
            case POOL:
                builder.setPool(MaxPoolingAggregatorSerializer.toSerializable((MaxPoolingAggregator) aggregator));
                break;
        }
        builder
            .setSampleSize(layer.sampleSize())
            .setRandomState(layer.randomState());

        return builder.build();
    }

    public static Layer fromSerializable(GraphSageProto.Layer protoLayer) {
        var aggregatorCase = protoLayer.getAggregatorCase();
        switch (aggregatorCase) {
            case MEAN:
                var meanAggregator = MeanAggregatorSerializer.fromSerializable(protoLayer.getMean());
                return new MeanAggregatingLayer(
                    new Weights<>(meanAggregator.weightsData()),
                    protoLayer.getSampleSize(),
                    meanAggregator.activationFunction(),
                    protoLayer.getRandomState()
                );
            case POOL:
                var maxPoolingAggregator = MaxPoolingAggregatorSerializer.fromSerializable(protoLayer.getPool());
                return new MaxPoolAggregatingLayer(
                    protoLayer.getSampleSize(),
                    new Weights<>(maxPoolingAggregator.poolWeights()),
                    new Weights<>(maxPoolingAggregator.selfWeights()),
                    new Weights<>(maxPoolingAggregator.neighborsWeights()),
                    new Weights<>(maxPoolingAggregator.bias()),
                    maxPoolingAggregator.activationFunction(),
                    protoLayer.getRandomState()
                );
            default:
                throw new IllegalArgumentException(formatWithLocale("Unknown aggregator: %s", aggregatorCase));
        }
    }
}
