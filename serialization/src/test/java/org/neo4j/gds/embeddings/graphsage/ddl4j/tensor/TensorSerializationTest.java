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
package org.neo4j.gds.embeddings.graphsage.ddl4j.tensor;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.model.proto.ProtoTensor;
import org.neo4j.graphalgo.utils.serialization.ProtoUtils;

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

        var iterableData = ProtoUtils.from(matrix.data());
        var protoMatrix = ProtoTensor.Matrix.newBuilder()
            .addAllData(iterableData)
            .setRows(matrix.rows())
            .setCols(matrix.cols())
            .build();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        protoMatrix.writeTo(output);

        var parsedProtoMatrix = ProtoTensor.Matrix.parseFrom(output.toByteArray());
        assertThat(parsedProtoMatrix).isNotNull();

        var parsedData = ProtoUtils.from(parsedProtoMatrix.getDataList());
        var deserializedMatrix = new Matrix(parsedData, parsedProtoMatrix.getRows(), parsedProtoMatrix.getCols());

        Assertions.assertThat(deserializedMatrix)
            .usingRecursiveComparison()
            .withStrictTypeChecking()
            .isEqualTo(matrix);

    }

}
