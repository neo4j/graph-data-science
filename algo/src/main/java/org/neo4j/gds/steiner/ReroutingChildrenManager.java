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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

public class ReroutingChildrenManager {

    private HugeObjectArray<LinkedNode> nodes;
    private BitSet isTerminal;

    ReroutingChildrenManager(long nodeCount, BitSet isTerminal) {
        nodes = HugeObjectArray.newArray(LinkedNode.class, nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; ++nodeId) {
            nodes.set(nodeId, LinkedNode.createChild(nodeId));
        }
        this.isTerminal = isTerminal;
    }

    void cut(long index) {
        LinkedNode node = nodes.get(index);
        LinkedNode siblingRight = node.siblingRight;
        LinkedNode siblingLeft = node.siblingLeft;

        boolean leftIsParent = siblingLeft.childRight != null && siblingLeft.childRight.index == index;

        if (leftIsParent) {
            siblingLeft.addChild(siblingRight);
        } else {
            siblingLeft.addSibling(siblingRight, Direction.RIGHT);
        }
        if (siblingRight != null) {
            siblingRight.addSibling(siblingLeft, Direction.LEFT);
        }
    }

    void link(long index, long parentIndex) {
        LinkedNode node = nodes.get(index);
        LinkedNode parent = nodes.get(parentIndex);
        LinkedNode parentRight = parent.childRight;
        node.addSibling(parent, Direction.LEFT);
        node.addSibling(parentRight, Direction.RIGHT);
        parent.addChild(node);
        if (parentRight != null) {
            parentRight.addSibling(node, Direction.LEFT);
        }
    }

    private boolean hasAtLeastTwoChildren(long index) {
        var node = nodes.get(index);
        if (node.childRight == null) {
            return false;
        }
        return node.childRight.siblingRight != null;
    }

    boolean prunable(long index) {
        return !isTerminal.get(index) && !hasAtLeastTwoChildren(index);
    }

}
