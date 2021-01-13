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
package org.neo4j.gds.embeddings.ddl4j.tensor;

import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Vector;
import org.neo4j.graphalgo.core.model.proto.ProtoTensor;
import org.neo4j.graphalgo.utils.serialization.ProtoUtils;

public final class TensorSerializer {

    private TensorSerializer() {}

    public static ProtoTensor.Matrix toSerializable(Matrix matrix) {
        return ProtoTensor.Matrix.newBuilder()
            .addAllData(ProtoUtils.toList(matrix.data()))
            .setRows(matrix.rows())
            .setCols(matrix.cols())
            .build();
    }

    public static Matrix fromSerializable(ProtoTensor.Matrix protoMatrix) {
        return new Matrix(
            ProtoUtils.toArray(protoMatrix.getDataList()),
            protoMatrix.getRows(),
            protoMatrix.getCols()
        );
    }

    static ProtoTensor.Vector toSerializable(Vector vector) {
        return ProtoTensor.Vector.newBuilder()
            .addAllData(ProtoUtils.toList(vector.data()))
            .build();
    }

    static Vector fromSerializable(ProtoTensor.Vector protoVector) {
        return new Vector(ProtoUtils.toArray(protoVector.getDataList()));
    }

    static ProtoTensor.Scalar toSerializable(Scalar scalar) {
        return ProtoTensor.Scalar.newBuilder()
            .setValue(scalar.value())
            .build();
    }

    static Scalar fromSerializable(ProtoTensor.Scalar protoScalar) {
        return new Scalar(protoScalar.getValue());
    }
}
