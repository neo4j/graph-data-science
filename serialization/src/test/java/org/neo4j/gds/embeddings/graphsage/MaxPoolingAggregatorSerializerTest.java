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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class MaxPoolingAggregatorSerializerTest {
    private static final Matrix MATRIX = new Matrix(
        new double[]{
            1, 2, 3,
            4, 5, 6
        },
        2, 3
    );

    private static final Vector VECTOR = new Vector(new double[]{1, 2, 3});
    private static final Weights<Vector> BIAS = new Weights<>(VECTOR);
    private static final Weights<Matrix> WEIGHTS = new Weights<>(MATRIX);

    @Test
    void canSerializeAndDeserialize() throws IOException {
        var aggregator = new MaxPoolingAggregator(
            WEIGHTS,
            WEIGHTS,
            WEIGHTS,
            BIAS,
            ActivationFunction.SIGMOID
        );

        var serializableAggregator = MaxPoolingAggregatorSerializer.toSerializable(aggregator);

        var byteArrayOutputStream = new ByteArrayOutputStream();
        serializableAggregator.writeTo(byteArrayOutputStream);

        var parsedAggregator = GraphSageProto.MaxPoolingAggregator.parseFrom(byteArrayOutputStream.toByteArray());
        var deserializedAggregator = MaxPoolingAggregatorSerializer.fromSerializable(parsedAggregator);

        assertThat(deserializedAggregator)
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(aggregator);
    }
}
