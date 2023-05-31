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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.huge.DirectIdMap;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeRelationshipsBatchBufferTest {

    @Test
    void shouldNotThrowOnCheckedBuffer() {
        var compositeBatchBuffer = createCompositeBuffer(true,2, 2);

        var type0Rel = ImmutableTestRelationship.builder()
            .typeTokenId(0)
            .relationshipId(0)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        var type1Rel = ImmutableTestRelationship.builder()
            .typeTokenId(1)
            .relationshipId(1)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        assertThat(compositeBatchBuffer.offer(type0Rel)).isTrue();
        assertThat(compositeBatchBuffer.offer(type1Rel)).isTrue();
        assertThat(compositeBatchBuffer.offer(type0Rel)).isFalse();
        assertThat(compositeBatchBuffer.offer(type1Rel)).isFalse();
        assertThat(compositeBatchBuffer.offer(type0Rel)).isFalse();
        assertThat(compositeBatchBuffer.offer(type1Rel)).isFalse();
        assertThat(compositeBatchBuffer.isFull()).isTrue();
    }

    @Test
    void shouldThrowOnUncheckedBuffer() {
        var compositeBatchBuffer = createCompositeBuffer(false,2, 2);

        var type0Rel = ImmutableTestRelationship.builder()
            .typeTokenId(0)
            .relationshipId(0)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        var type1Rel = ImmutableTestRelationship.builder()
            .typeTokenId(1)
            .relationshipId(1)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        assertThat(compositeBatchBuffer.offer(type0Rel)).isTrue();
        assertThat(compositeBatchBuffer.offer(type1Rel)).isTrue();
        assertThat(compositeBatchBuffer.offer(type0Rel)).isTrue();
        assertThat(compositeBatchBuffer.offer(type1Rel)).isTrue();
        assertThatThrownBy(() -> compositeBatchBuffer.offer(type0Rel)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThatThrownBy(() -> compositeBatchBuffer.offer(type1Rel)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThat(compositeBatchBuffer.isFull()).isTrue();
    }

    private static RecordsBatchBuffer<RelationshipReference> createCompositeBuffer(boolean checked, int typeCount, int capacity) {
        var buffers = IntStream.range(0, typeCount)
            .mapToObj(type -> new RelationshipsBatchBufferBuilder()
                .idMap(new DirectIdMap(2))
                .type(type)
                .capacity(capacity)
                .useCheckedBuffer(checked)
                .build())
            .toArray(RelationshipsBatchBuffer[]::new);

        return new CompositeRelationshipsBatchBufferBuilder()
            .buffers(buffers)
            .useCheckedBuffer(checked)
            .build();
    }

}
