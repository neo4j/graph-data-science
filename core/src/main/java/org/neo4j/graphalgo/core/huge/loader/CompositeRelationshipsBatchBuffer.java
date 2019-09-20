/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public final class CompositeRelationshipsBatchBuffer extends RecordsBatchBuffer<RelationshipRecord> {

    private final RelationshipsBatchBuffer[] buffers;

    private CompositeRelationshipsBatchBuffer(RelationshipsBatchBuffer... buffers) {
        super(0);
        this.buffers = buffers;
    }

    static RecordsBatchBuffer<RelationshipRecord> of(RelationshipsBatchBuffer... buffers) {
        if (buffers.length == 1) {
            return buffers[0];
        }
        return new CompositeRelationshipsBatchBuffer(buffers);
    }

    @Override
    public void offer(RelationshipRecord record) {
        for (RelationshipsBatchBuffer buffer : buffers) {
            buffer.offer(record);
        }
    }

    @Override
    public void reset() {
        for (RelationshipsBatchBuffer buffer : buffers) {
            buffer.reset();
        }
    }
}
