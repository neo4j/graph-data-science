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
package org.neo4j.gds.embeddings.ddl4j.tensor;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.gds.core.model.proto.TensorProto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TensorSerializationTest {

    @Test
    void canSerializeAndDeserializeMatrix() throws IOException {
        var matrix = new Matrix(
            new double[]{
                1, 2, 3,
                4, 5, 6
            },
            2, 3
        );

        var protoMatrix = TensorSerializer.toSerializable(matrix);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        protoMatrix.writeTo(output);

        var parsedProtoMatrix = TensorProto.Matrix.parseFrom(output.toByteArray());
        assertThat(parsedProtoMatrix).isNotNull();

        var deserializedMatrix = TensorSerializer.fromSerializable(parsedProtoMatrix);

        Assertions.assertThat(deserializedMatrix)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(matrix);
    }

    @Test
    void canSerializeAndDeserializeVector() throws IOException {
        var vector = new Vector(new double[]{1, 2, 3, 4, 5, 6});

        var protoVector = TensorSerializer.toSerializable(vector);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        protoVector.writeTo(output);

        var parsedProtoVector = TensorProto.Vector.parseFrom(output.toByteArray());
        assertThat(parsedProtoVector).isNotNull();

        var deserializedVector = TensorSerializer.fromSerializable(parsedProtoVector);

        Assertions.assertThat(deserializedVector)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(vector);
    }

    @Test
    void canSerializeAndDeserializeScalar() throws IOException {
        var scalar = new Scalar(13.37);

        var protoScalar = TensorSerializer.toSerializable(scalar);

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        protoScalar.writeTo(output);

        var parsedProtoScalar = TensorProto.Scalar.parseFrom(output.toByteArray());
        assertThat(parsedProtoScalar).isNotNull();

        var deserializedScalar = TensorSerializer.fromSerializable(parsedProtoScalar);

        Assertions.assertThat(deserializedScalar)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(scalar);
    }

}
