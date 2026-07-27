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
package org.neo4j.gds.procedures.algorithms.embeddings.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.ml.core.tensor.FloatVector;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class Node2VecPropertiesTest {

    @Test
    void shouldCreateCorrectProperties() {

        float[] vector1 = {1, 2, 3};
        float[] vector2 = {4, 5, 6};
        var node2VecResult = new Node2VecResult(
            HugeObjectArray.of(
                new FloatVector(vector1),
                new FloatVector(vector2)
            ), List.of()
        );
        var asProps= Node2VecProperties.create(node2VecResult);
        assertThat(asProps.nodeCount()).isEqualTo(2L);
        assertThat(asProps.floatArrayValue(0)).isEqualTo(vector1);
        assertThat(asProps.floatArrayValue(1)).isEqualTo(vector2);

    }

}
