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

public class CompositeRelationshipsBatchBuffer extends RecordsBatchBuffer<RelationshipReference> {

    protected final RelationshipsBatchBuffer[] buffers;

    private CompositeRelationshipsBatchBuffer(RelationshipsBatchBuffer... buffers) {
        super(0);
        this.buffers = buffers;
    }

    public static RecordsBatchBuffer<RelationshipReference> of(RelationshipsBatchBuffer... buffers) {
        if (buffers.length == 1) {
            return buffers[0];
        }
        return new CompositeRelationshipsBatchBuffer(buffers);
    }

    @Override
    public boolean offer(RelationshipReference record) {
        for (RelationshipsBatchBuffer buffer : buffers) {
            buffer.offer(record);
        }
        return true;
    }

    @Override
    public void reset() {
        for (RelationshipsBatchBuffer buffer : buffers) {
            buffer.reset();
        }
    }

    /**
     * A version of a relationships batch buffer that checks the
     * buffer length before inserting a new record. This is necessary
     * in the case where the number of records offered to this
     * buffer can exceed the configured batch size.
     */
    private static final class Checked extends CompositeRelationshipsBatchBuffer {

        Checked(RelationshipsBatchBuffer... buffers) {
            super(buffers);
        }

        @Override
        public boolean offer(RelationshipReference record) {
            for (RelationshipsBatchBuffer buffer : buffers) {
                if (!buffer.offer(record)) {
                    return false;
                }
            }
            return true;
        }
    }
}
