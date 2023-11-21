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
import org.neo4j.gds.core.loading.RecordsBatchBuffer;

public final class BufferedCompositeRelationshipConsumer extends RecordsBatchBuffer implements StoreScanner.RecordConsumer<RelationshipReference> {

    private final BufferedRelationshipConsumer[] buffers;

    @Builder.Factory
    static StoreScanner.RecordConsumer<RelationshipReference> bufferedCompositeRelationshipConsumer(
        BufferedRelationshipConsumer[] buffers
    ) {
        if (buffers.length == 1) {
            return buffers[0];
        }
        return new BufferedCompositeRelationshipConsumer(buffers);
    }

    private BufferedCompositeRelationshipConsumer(BufferedRelationshipConsumer... buffers) {
        super(0);
        this.buffers = buffers;
    }

    @Override
    public boolean offer(RelationshipReference record) {
        boolean offered = true;
        for (BufferedRelationshipConsumer buffer : buffers) {
            offered = buffer.offer(record) && offered;
        }
        return offered;
    }

    @Override
    public void reset() {
        for (BufferedRelationshipConsumer buffer : buffers) {
            buffer.reset();
        }
    }
}
