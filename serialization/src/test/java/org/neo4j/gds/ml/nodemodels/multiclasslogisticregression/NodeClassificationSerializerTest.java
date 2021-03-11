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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.graphalgo.core.model.proto.TensorProto;
import org.neo4j.graphalgo.ml.model.proto.NodeClassificationProto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NodeClassificationSerializerTest {

    @Test
    void shouldSerializeMultiClassNLRData() {
        var weightData = Matrix.fill(0.5D, 3, 4);
        var weight = new Weights<>(weightData);
        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(101L);
        classIdMap.toMapped(19L);
        classIdMap.toMapped(42L);

        var modelData = MultiClassNLRData.builder()
            .weights(weight)
            .classIdMap(classIdMap)
            .build();

        var serializer = new NodeClassificationSerializer();
        var serializedData = serializer.toSerializable(modelData);

        var serializedWeights = serializedData.getWeights();
        var serializedArray = serializedWeights.getDataList().stream().mapToDouble(Double::doubleValue).toArray();
        assertThat(serializedArray).containsExactly(weightData.data());

        assertThat(serializedWeights.getRows()).isEqualTo(weightData.rows());
        assertThat(serializedWeights.getCols()).isEqualTo(weightData.cols());

        var localIdMap = serializedData
            .getLocalIdMap()
            .getOriginalIdsList()
            .stream()
            .mapToLong(Long::longValue)
            .toArray();
        assertThat(localIdMap).containsExactly(101L, 19L, 42L);
    }

    @Test
    void shouldDeserializeMultiClassNLRData() {
        var meh = new ArrayList<Double>(12);
        Collections.fill(meh, 0.5D);
        var serialized =
            NodeClassificationProto.NodeClassificationModelData.newBuilder()
                .setWeights(TensorProto.Matrix.newBuilder().addAllData(meh))
                .setLocalIdMap(NodeClassificationProto.LocalIdMap
                    .newBuilder()
                    .addAllOriginalIds(List.of(19L, 42L))
                    .build())
                .build();

        var serializer = new NodeClassificationSerializer();
        var multiClassNLRData = serializer.deserializeModelData(serialized);

        assertThat(multiClassNLRData).isNotNull();

        assertThat(multiClassNLRData.classIdMap().originalIds()).containsExactly(19L, 42L);

        assertThat(multiClassNLRData.weights().data().data()).containsExactly(meh
            .stream()
            .mapToDouble(Double::doubleValue)
            .toArray());
    }
}
