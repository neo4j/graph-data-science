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

import org.neo4j.gds.compat.CompositeNodeCursor;
import org.neo4j.gds.core.loading.NodeLabelTokenSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.storageengine.api.LongReference;
import org.neo4j.storageengine.api.Reference;

public class MultipleNodeLabelIndexReference implements NodeReference {

    private final CompositeNodeCursor compositeNodeCursor;
    private final Read dataRead;
    private final NodeCursor nodeCursor;

    MultipleNodeLabelIndexReference(
        CompositeNodeCursor compositeNodeCursor,
        Read dataRead,
        NodeCursor nodeCursor
    ) {
        this.compositeNodeCursor = compositeNodeCursor;
        this.dataRead = dataRead;
        this.nodeCursor = nodeCursor;
    }

    @Override
    public long nodeId() {
        return compositeNodeCursor.nodeReference();
    }

    @Override
    public NodeLabelTokenSet labels() {
        return NodeLabelTokenSet.from(compositeNodeCursor.currentLabels());
    }

    @Override
    public long relationshipReference() {
        dataRead.singleNode(compositeNodeCursor.nodeReference(), nodeCursor);
        if (nodeCursor.next()) {
            return nodeCursor.relationshipsReference();
        } else {
            return LongReference.NULL;
        }
    }

    @Override
    public Reference propertiesReference() {
        dataRead.singleNode(compositeNodeCursor.nodeReference(), nodeCursor);
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
