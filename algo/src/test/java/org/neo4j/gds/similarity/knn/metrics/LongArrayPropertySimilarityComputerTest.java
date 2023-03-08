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
package org.neo4j.gds.similarity.knn.metrics;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;

import static org.assertj.core.api.Assertions.assertThat;

class LongArrayPropertySimilarityComputerTest {

    @Test
    void usingSparseProperties() {
        long[][] inputValues = {{42, 24}, null, {84, 48}};

        var sparseProperties = new LongArrayNodePropertyValues() {
            @Override
            public long[] longArrayValue(long nodeId) {
                return inputValues[(int) nodeId];
            }

            @Override
            public long nodeCount() {
                return inputValues.length;
            }
        };

        var sortedValues = new LongArrayPropertySimilarityComputer.SortedLongArrayPropertyValues(sparseProperties);

        assertThat(sortedValues.longArrayValue(0)).containsExactly(24, 42);
        assertThat(sortedValues.longArrayValue(1)).isNull();
        assertThat(sortedValues.longArrayValue(2)).containsExactly(48, 84);
    }

}
