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

import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.Reference;

public class NodeLabelIndexReference implements NodeReference {

    private final NodeLabelIndexCursor labelIndexCursor;
    private final Read dataRead;
    private final NodeCursor nodeCursor;
    private final NodeLabelTokenSet labelTokens;

    NodeLabelIndexReference(NodeLabelIndexCursor labelIndexCursor, Read dataRead, NodeCursor nodeCursor, int labelId) {
        this.labelIndexCursor = labelIndexCursor;
        this.dataRead = dataRead;
        this.nodeCursor = nodeCursor;
        this.labelTokens = NodeLabelTokenSet.from(labelId);
    }

    @Override
    public long nodeId() {
        return labelIndexCursor.nodeReference();
    }

    @Override
    public NodeLabelTokenSet labels() {
        return labelTokens;
    }

    @Override
    public long relationshipReference() {
        dataRead.singleNode(labelIndexCursor.nodeReference(), nodeCursor);
        if (nodeCursor.next()) {
            return nodeCursor.relationshipsReference();
        } else {
            return LongReference.NULL;
        }
    }

    @Override
    public Reference propertiesReference() {
        dataRead.singleNode(labelIndexCursor.nodeReference(), nodeCursor);
        if (nodeCursor.next()) {
            return nodeCursor.propertiesReference();
        } else {
            return LongReference.NULL_REFERENCE;
        }
    }

    void close() {
        nodeCursor.close();
    }
}
