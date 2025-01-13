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
package org.neo4j.gds.hdbscan;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.stream.LongStream;

public class KdTree {

    private final HugeLongArray ids;
    private final NodePropertyValues nodePropertyValues;
    private final KdNode root;

    KdTree(HugeLongArray ids, NodePropertyValues nodePropertyValues, KdNode root) {
        this.ids = ids;
        this.nodePropertyValues = nodePropertyValues;
        this.root = root;
    }

    KdNode root() {
        return root;
    }

    KdNode parent(KdNode kdNode) {
        return kdNode.parent();
    }

    KdNode leftChild(KdNode kdNode) {
        return kdNode.leftChild();
    }

    KdNode rightChild(KdNode kdNode) {
        return kdNode.rightChild();
    }

    KdNode sibling(KdNode kdNode) {
        return kdNode.sibling();
    }

    LongStream nodesContained(KdNode node) {
        var start = node.start();
        var end = node.end();
        return LongStream.range(start, end).map(ids::get);
    }

}
