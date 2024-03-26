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
import org.immutables.builder.Builder;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.gds.core.loading.NodesBatchBuffer;
import org.neo4j.gds.core.loading.NodesBatchBufferBuilder;
import org.neo4j.token.api.TokenConstants;

import java.util.Optional;

import static org.neo4j.gds.utils.GdsFeatureToggles.SKIP_ORPHANS;
import static org.neo4j.internal.kernel.api.Read.NO_ID;

public final class BufferedNodeConsumer implements StoreScanner.RecordConsumer<NodeReference> {

    private final NodesBatchBuffer<PropertyReference> buffer;

    private final long highestPossibleNodeCount;
    private final LongSet nodeLabelIds;
    private final boolean skipOrphans;
    private final boolean readProperty;

    @Builder.Factory
    static BufferedNodeConsumer bufferedNodeConsumer(
        int capacity,
        long highestPossibleNodeCount,
        Optional<LongSet> nodeLabelIds,
        Optional<Boolean> hasLabelInformation,
        Optional<Boolean> readProperty
    ) {
        var buffer = new NodesBatchBufferBuilder<PropertyReference>()
            .capacity(capacity)
            .hasLabelInformation(hasLabelInformation)
            .readProperty(readProperty)
            .propertyReferenceClass(PropertyReference.class)
            .build();

        LongSet labelIds = nodeLabelIds.orElseGet(LongHashSet::new);
        boolean readProps = readProperty.orElse(false);

        return new BufferedNodeConsumer(
            buffer,
            highestPossibleNodeCount,
            labelIds,
            readProps
        );
    }

    private BufferedNodeConsumer(
        NodesBatchBuffer<PropertyReference> buffer,
        long highestPossibleNodeCount,
        LongSet nodeLabelIds,
        boolean readProperty
        ) {
        this.buffer = buffer;
        this.highestPossibleNodeCount = highestPossibleNodeCount;
        this.nodeLabelIds = nodeLabelIds;
        this.skipOrphans = SKIP_ORPHANS.isEnabled();
        this.readProperty = readProperty;
    }

    NodesBatchBuffer<PropertyReference> nodesBatchBuffer() {
        return this.buffer;
    }

    @Override
    public boolean offer(NodeReference record) {
        if (this.buffer.isFull()) {
            return false;
        }

        if (this.skipOrphans && record.relationshipReference() == NO_ID) {
            return true;
        }

        if (record.nodeId() >= this.highestPossibleNodeCount) {
            return true;
        }

        if (this.nodeLabelIds.isEmpty()) {
            var propertiesReference = this.readProperty
                ? record.propertiesReference()
                : Neo4jProxy.noPropertyReference();
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
                var propertiesReference = this.readProperty
                    ? record.propertiesReference()
                    : Neo4jProxy.noPropertyReference();

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
