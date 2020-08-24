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

import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

public final class NodeRecordReference implements NodeReference {

    private final NodeRecord record;
    private final NodeStore nodeStore;
    private final PageCursorTracer cursorTracer;

    NodeRecordReference(
        NodeRecord record,
        NodeStore nodeStore,
        PageCursorTracer cursorTracer
    ) {
        this.record = record;
        this.nodeStore = nodeStore;
        this.cursorTracer = cursorTracer;
    }

    @Override
    public long nodeId() {
        return record.getId();
    }

    @Override
    public long[] labels() {
        return Neo4jProxy.getNodeLabelFields(record, nodeStore, cursorTracer);
    }

    @Override
    public long relationshipReference() {
        return record.getNextRel();
    }

    @Override
    public long propertiesReference() {
        return record.getNextProp();
    }
}
