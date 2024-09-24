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

import com.carrotsearch.hppc.LongHashSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.Reference;

import static org.assertj.core.api.Assertions.assertThat;

class BufferedNodeConsumerTest {

    static int bufferLength(BufferedNodeConsumer b) {
        return b.nodesBatchBuffer().length();
    }

    static long[] bufferBatch(BufferedNodeConsumer b) {
        return b.nodesBatchBuffer().batch();
    }

    @Test
    void shouldIgnoreNodesThatAreOutOfBoundsOnOffer() {
        var buffer = new BufferedNodeConsumerBuilder()
            .capacity(3)
            .highestPossibleNodeCount(43)
            .build();

        // within range
        buffer.offer(new TestNode(21));
        // end of range
        buffer.offer(new TestNode(42));
        // out of range
        buffer.offer(new TestNode(84));

        assertThat(buffer)
            .returns(2, BufferedNodeConsumerTest::bufferLength)
            .returns(new long[]{21, 42, 0}, BufferedNodeConsumerTest::bufferBatch);
    }

    @Test
    void shouldIgnoreNodesThatAreOutOfBoundsOnOfferWithLabelInformation() {
        var nodesBatchBuffer = new BufferedNodeConsumerBuilder()
            .capacity(3)
            .highestPossibleNodeCount(43)
            .hasLabelInformation(true)
            .nodeLabelIds(LongHashSet.from(0))
            .build();

        // within range
        nodesBatchBuffer.offer(new TestNode(21, 0));
        // end of range
        nodesBatchBuffer.offer(new TestNode(42, 0));
        // out of range
        nodesBatchBuffer.offer(new TestNode(84, 0));

        assertThat(nodesBatchBuffer)
            .returns(2, BufferedNodeConsumerTest::bufferLength)
            .returns(new long[]{21, 42, 0}, BufferedNodeConsumerTest::bufferBatch);
    }

    @Test
    void shouldNotThrowWhenFull() {
        var nodesBatchBuffer = new BufferedNodeConsumerBuilder()
            .capacity(2)
            .highestPossibleNodeCount(42)
            .build();

        assertThat(nodesBatchBuffer.offer(new TestNode(0))).isTrue();
        assertThat(nodesBatchBuffer.offer(new TestNode(1))).isFalse();
        assertThat(nodesBatchBuffer.offer(new TestNode(2))).isFalse();
        assertThat(nodesBatchBuffer.nodesBatchBuffer().isFull()).isTrue();
    }

    private static final class TestNode implements NodeReference {
        private final long nodeId;
        private final long[] labels;

        private TestNode(long nodeId, long... labels) {
            this.nodeId = nodeId;
            this.labels = labels;
        }

        @Override
        public long nodeId() {
            return this.nodeId;
        }

        @Override
        public NodeLabelTokenSet labels() {
            return NodeLabelTokenSet.from(labels);
        }

        @Override
        public long relationshipReference() {
            return -1L;
        }

        @Override
        public Reference propertiesReference() {
            return LongReference.NULL_REFERENCE;
        }
    }
}
