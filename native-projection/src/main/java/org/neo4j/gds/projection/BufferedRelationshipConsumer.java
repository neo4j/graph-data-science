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

import org.immutables.builder.Builder;
import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.core.loading.RelationshipsBatchBuffer;
import org.neo4j.gds.core.loading.RelationshipsBatchBufferBuilder;
import org.neo4j.storageengine.api.Reference;

import java.util.Optional;

import static org.neo4j.gds.api.IdMap.NOT_FOUND;
import static org.neo4j.gds.core.loading.LoadingExceptions.validateSourceNodeIsLoaded;
import static org.neo4j.gds.core.loading.LoadingExceptions.validateTargetNodeIsLoaded;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

public final class BufferedRelationshipConsumer implements StoreScanner.RecordConsumer<RelationshipReference> {

    private final RelationshipsBatchBuffer<Reference> buffer;

    private final PartialIdMap idMap;
    private final int type;
    private final boolean skipDanglingRelationships;

    @Builder.Factory
    static BufferedRelationshipConsumer bufferedRelationshipConsumer(
        PartialIdMap idMap,
        int type,
        int capacity,
        Optional<Boolean> skipDanglingRelationships
    ) {
        var buffer = new RelationshipsBatchBufferBuilder<Reference>()
            .capacity(capacity)
            .propertyReferenceClass(Reference.class)
            .build();

        boolean skipDangling = skipDanglingRelationships.orElse(true);

        return new BufferedRelationshipConsumer(buffer, idMap, type, skipDangling);
    }

    private BufferedRelationshipConsumer(
        RelationshipsBatchBuffer<Reference> buffer,
        PartialIdMap idMap,
        int type,
        boolean skipDanglingRelationships
    ) {
        this.buffer = buffer;
        this.idMap = idMap;
        this.type = type;
        this.skipDanglingRelationships = skipDanglingRelationships;
    }

    public RelationshipsBatchBuffer<Reference> relationshipsBatchBuffer() {
        return this.buffer;
    }

    @Override
    public boolean offer(RelationshipReference record) {
        if (this.buffer.isFull()) {
            return false;
        }
        if ((type == ANY_RELATIONSHIP_TYPE) || (type == record.typeTokenId())) {
            long source = idMap.toMappedNodeId(record.sourceNodeReference());
            long target = idMap.toMappedNodeId(record.targetNodeReference());

            if (source == NOT_FOUND || target == NOT_FOUND) {
                if (skipDanglingRelationships) {
                    return true;
                }
                validateSourceNodeIsLoaded(source, record.sourceNodeReference());
                validateTargetNodeIsLoaded(target, record.targetNodeReference());
            }

            this.buffer.add(source, target, record.relationshipId(), record.propertiesReference());
        }
        return !this.buffer.isFull();

    }

    @Override
    public void reset() {
        this.buffer.reset();
    }
}
