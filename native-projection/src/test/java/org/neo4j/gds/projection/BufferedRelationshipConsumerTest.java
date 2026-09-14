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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.GenerateBuilder;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.Reference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BufferedRelationshipConsumerTest {

    @Test
    void flushBufferWhenFull() {
        var buffer = BufferedRelationshipConsumer.of(new DirectIdMap(2), -1, 1);
        buffer.relationshipsBatchBuffer().add(0, 1, -1, LongReference.NULL_REFERENCE);
        assertTrue(buffer.relationshipsBatchBuffer().isFull());
    }

    @Test
    void shouldNotThrowWhenFull() {
        var relationshipsBatchBuffer = BufferedRelationshipConsumer.of(new DirectIdMap(2), -1, 2);

        var testRelationship = TestRelationshipBuilder.builder()
            .typeTokenId(0)
            .relationshipId(0)
            .sourceNodeReference(0)
            .targetNodeReference(1)
            .build();

        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isTrue();
        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isFalse();
        assertThat(relationshipsBatchBuffer.offer(testRelationship)).isFalse();
        assertThat(relationshipsBatchBuffer.relationshipsBatchBuffer().isFull()).isTrue();
    }

    @GenerateBuilder
    record TestRelationship(
        long relationshipId,
        int typeTokenId,
        long sourceNodeReference,
        long targetNodeReference
    ) implements RelationshipReference {
        @Override
        public Reference propertiesReference() {
            return LongReference.NULL_REFERENCE;
        }
    }

}
