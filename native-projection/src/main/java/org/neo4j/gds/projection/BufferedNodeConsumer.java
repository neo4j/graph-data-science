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
import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.gds.core.loading.NodesBatchBuffer;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.token.api.TokenConstants;

public final class BufferedNodeConsumer implements StoreScanner.RecordConsumer<NodeReference> {

    private final NodesBatchBuffer<Reference> buffer;

    private final long highestPossibleNodeCount;
    private final LongSet nodeLabelIds;
    private final boolean readProperty;

    public static BufferedNodeConsumer of(int capacity, long highestPossibleNodeCount) {
        return of(capacity, highestPossibleNodeCount, new LongHashSet(), false, false);
    }

    public static BufferedNodeConsumer of(
        int capacity,
        long highestPossibleNodeCount,
        LongSet nodeLabelIds,
        boolean hasLabelInformation,
        boolean readProperty
    ) {
        var buffer = NodesBatchBuffer.of(capacity, hasLabelInformation, readProperty, Reference.class);
        return new BufferedNodeConsumer(buffer, highestPossibleNodeCount, nodeLabelIds, readProperty);
    }

    private BufferedNodeConsumer(
        NodesBatchBuffer<Reference> buffer,
        long highestPossibleNodeCount,
        LongSet nodeLabelIds,
        boolean readProperty
        ) {
        this.buffer = buffer;
        this.highestPossibleNodeCount = highestPossibleNodeCount;
        this.nodeLabelIds = nodeLabelIds;
        this.readProperty = readProperty;
    }

    NodesBatchBuffer<Reference> nodesBatchBuffer() {
        return this.buffer;
    }

    @Override
    public boolean offer(NodeReference record) {
        if (this.buffer.isFull()) {
            return false;
        }

        if (record.nodeId() >= this.highestPossibleNodeCount) {
            return true;
        }

        if (this.nodeLabelIds.isEmpty()) {
            Reference propertiesReference;
            propertiesReference = this.readProperty ? record.propertiesReference() : LongReference.NULL_REFERENCE;
            this.buffer.add(record.nodeId(), propertiesReference, NodeLabelTokenSet.ANY_LABEL);
        } else {
            boolean atLeastOneLabelFound = false;
            var labels = record.labels();
            for (int i = 0; i < labels.length(); i++) {
                long l = labels.get(i);
                if (!nodeLabelIds.contains(l) && !nodeLabelIds.contains(TokenConstants.ANY_LABEL)) {
                    labels.ignore(i);
                } else {
                    atLeastOneLabelFound = true;
                }
            }
            if (atLeastOneLabelFound) {
                Reference propertiesReference;
                propertiesReference = this.readProperty ? record.propertiesReference() : LongReference.NULL_REFERENCE;

                this.buffer.add(record.nodeId(), propertiesReference, labels);
            }
        }
        return !this.buffer.isFull();
    }

    @Override
    public void reset() {
        this.buffer.reset();
    }
}
