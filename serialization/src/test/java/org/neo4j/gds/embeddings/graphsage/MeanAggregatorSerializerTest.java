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
import org.neo4j.graphalgo.core.model.proto.GraphSageProto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class MeanAggregatorSerializerTest {

    private static final Matrix MATRIX = new Matrix(
        new double[]{
            1, 2, 3,
            4, 5, 6
        },
        2, 3
    );

    @Test
    void canSerialize() throws IOException {
        var weights = new Weights<>(MATRIX);
        var aggregator = new MeanAggregator(
            weights,
            ActivationFunction.SIGMOID
        );

        var serializableMeanAggregator = MeanAggregatorSerializer.toSerializable(
            aggregator
        );

        var byteArrayOutputStream = new ByteArrayOutputStream();
        var bytesBeforeWrite = byteArrayOutputStream.toByteArray();
        assertThat(bytesBeforeWrite).isEmpty();
        serializableMeanAggregator.writeTo(byteArrayOutputStream);

        var bytesAfterWrite = byteArrayOutputStream.toByteArray();
        assertThat(bytesAfterWrite).isNotEmpty();
    }

    @Test
    void canDeserialize() throws IOException {
        var weights = new Weights<>(MATRIX);
        var aggregator = new MeanAggregator(
            weights,
            ActivationFunction.SIGMOID
        );

        var serializableMeanAggregator = MeanAggregatorSerializer.toSerializable(aggregator);
        var byteArrayOutputStream = new ByteArrayOutputStream();
        serializableMeanAggregator.writeTo(byteArrayOutputStream);

        var parsedProtoAggregator = GraphSageProto.MeanAggregator.parseFrom(byteArrayOutputStream.toByteArray());

        var deserializedAggregator = MeanAggregatorSerializer.fromSerializable(parsedProtoAggregator);

        assertThat(deserializedAggregator)
            .isNotNull()
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(aggregator);
    }

}
