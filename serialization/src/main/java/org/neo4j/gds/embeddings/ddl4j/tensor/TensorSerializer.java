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

import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;
import org.neo4j.gds.ml.core.tensor.Vector;
import org.neo4j.graphalgo.core.model.proto.TensorProto;
import org.neo4j.graphalgo.utils.ProtoUtils;

public final class TensorSerializer {

    private TensorSerializer() {}

    public static TensorProto.Matrix toSerializable(Matrix matrix) {
        return TensorProto.Matrix.newBuilder()
            .addAllData(ProtoUtils.toList(matrix.data()))
            .setRows(matrix.rows())
            .setCols(matrix.cols())
            .build();
    }

    public static Matrix fromSerializable(TensorProto.Matrix protoMatrix) {
        return new Matrix(
            ProtoUtils.toArray(protoMatrix.getDataList()),
            protoMatrix.getRows(),
            protoMatrix.getCols()
        );
    }

    public static TensorProto.Vector toSerializable(Vector vector) {
        return TensorProto.Vector.newBuilder()
            .addAllData(ProtoUtils.toList(vector.data()))
            .build();
    }

    public static Vector fromSerializable(TensorProto.Vector protoVector) {
        return new Vector(ProtoUtils.toArray(protoVector.getDataList()));
    }

    static TensorProto.Scalar toSerializable(Scalar scalar) {
        return TensorProto.Scalar.newBuilder()
            .setValue(scalar.value())
            .build();
    }

    static Scalar fromSerializable(TensorProto.Scalar protoScalar) {
        return new Scalar(protoScalar.getValue());
    }
}
