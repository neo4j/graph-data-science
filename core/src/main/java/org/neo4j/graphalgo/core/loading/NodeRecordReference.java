/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

public final class NodeRecordReference implements NodeReference {

    private final NodeRecord record;
    private final NodeStore nodeStore;

    NodeRecordReference(NodeRecord record, NodeStore nodeStore) {
        this.record = record;
        this.nodeStore = nodeStore;
    }

    @Override
    public long nodeId() {
        return record.getId();
    }

    @Override
    public long[] labels() {
        // TODO: PageCursorTracer from tx
        return NodeLabelsField.get(record, nodeStore, PageCursorTracer.NULL);
    }

    @Override
    public long propertiesReference() {
        return record.getNextProp();
    }
}
