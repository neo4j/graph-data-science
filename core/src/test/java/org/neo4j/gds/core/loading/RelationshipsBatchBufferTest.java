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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.huge.DirectIdMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipsBatchBufferTest {

    @Test
    void flushBufferWhenFull() {
        var buffer = new RelationshipsBatchBufferBuilder()
            .idMap(new DirectIdMap(2))
            .type(-1)
            .capacity(1)
            .build();

        buffer.add(0, 1, -1, Neo4jProxy.noPropertyReference());
        assertTrue(buffer.isFull());
    }

    @Test
    void shouldNotThrowOnCheckedBuffer() {
        var relationshipsBatchBuffer = new RelationshipsBatchBufferBuilder()
            .idMap(new DirectIdMap(2))
            .type(-1)
            .capacity(2)
            .useCheckedBuffer(true)
            .build();

        var testRelationship = ImmutableTestRelationship.builder()
            .typeTokenId(0)
            .relationshipId(0)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isTrue();
        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isFalse();
        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isFalse();
        assertThat(relationshipsBatchBuffer.isFull()).isTrue();
    }

    @Test
    void shouldThrowOnUncheckedBuffer() {
        var relationshipsBatchBuffer = new RelationshipsBatchBufferBuilder()
            .idMap(new DirectIdMap(2))
            .type(-1)
            .capacity(2)
            .useCheckedBuffer(false)
            .build();

        var testRelationship = ImmutableTestRelationship.builder()
            .typeTokenId(0)
            .relationshipId(0)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isTrue();
        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isTrue();
        assertThatThrownBy(() -> relationshipsBatchBuffer.offer(testRelationship)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThat(relationshipsBatchBuffer.isFull()).isTrue();
    }


    @ValueClass
    public interface TestRelationship extends RelationshipReference {

        @Override
        default PropertyReference propertiesReference() {
            return Neo4jProxy.noPropertyReference();
        }
    }

}
