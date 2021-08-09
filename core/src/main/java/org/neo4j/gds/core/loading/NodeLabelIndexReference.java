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

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.PropertyReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;

public class NodeLabelIndexReference implements NodeReference {

    private final NodeLabelIndexCursor labelIndexCursor;
    private final Read dataRead;
    private final NodeCursor nodeCursor;
    private final long[] labelIds;

    NodeLabelIndexReference(NodeLabelIndexCursor labelIndexCursor, Read dataRead, NodeCursor nodeCursor, int labelId) {
        this.labelIndexCursor = labelIndexCursor;
        this.dataRead = dataRead;
        this.nodeCursor = nodeCursor;
        this.labelIds = new long[]{labelId};
    }

    @Override
    public long nodeId() {
        return labelIndexCursor.nodeReference();
    }

    @Override
    public long[] labels() {
        return labelIds;
    }

    @Override
    public long relationshipReference() {
        dataRead.singleNode(labelIndexCursor.nodeReference(), nodeCursor);
        if (nodeCursor.next()) {
            return Neo4jProxy.relationshipsReference(nodeCursor);
        } else {
            return Read.NO_ID;
        }
    }

    @Override
    public PropertyReference propertiesReference() {
        dataRead.singleNode(labelIndexCursor.nodeReference(), nodeCursor);
        if (nodeCursor.next()) {
            return Neo4jProxy.propertyReference(nodeCursor);
        } else {
            return Neo4jProxy.noPropertyReference();
        }
    }

    void close() {
        nodeCursor.close();
    }
}
