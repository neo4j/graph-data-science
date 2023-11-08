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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.loading.RelationshipReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BufferedRelationshipConsumerTest {

    @Test
    void flushBufferWhenFull() {
        var buffer = new BufferedRelationshipConsumerBuilder()
            .idMap(new DirectIdMap(2))
            .type(-1)
            .capacity(1)
            .build();

        buffer.relationshipsBatchBuffer().add(0, 1, -1, Neo4jProxy.noPropertyReference());
        assertTrue(buffer.relationshipsBatchBuffer().isFull());
    }

    @Test
    void shouldNotThrowWhenFull() {
        var relationshipsBatchBuffer = new BufferedRelationshipConsumerBuilder()
            .idMap(new DirectIdMap(2))
            .type(-1)
            .capacity(2)
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
        assertThat(relationshipsBatchBuffer.relationshipsBatchBuffer().isFull()).isTrue();
    }

    @ValueClass
    public interface TestRelationship extends RelationshipReference {

        @Override
        default PropertyReference propertiesReference() {
            return Neo4jProxy.noPropertyReference();
        }
    }

}
