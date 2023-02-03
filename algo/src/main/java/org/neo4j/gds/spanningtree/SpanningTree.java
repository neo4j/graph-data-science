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
package org.neo4j.gds.spanningtree;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.Objects;

/**
 * group of nodes that form a spanning tree
 */
public class SpanningTree {

    final long head;
    final long nodeCount;
    final long effectiveNodeCount;
    final HugeDoubleArray costToParent;
    final HugeLongArray parent;
    final double totalWeight;

    public SpanningTree(
        long head,
        long nodeCount,
        long effectiveNodeCount,
        HugeLongArray parent,
        HugeDoubleArray costToParent,
        double totalWeight
    ) {
        this.head = head;
        this.nodeCount = nodeCount;
        this.effectiveNodeCount = effectiveNodeCount;
        this.parent = parent;
        this.costToParent = costToParent;
        this.totalWeight = totalWeight;
    }

    public long effectiveNodeCount() {
        return effectiveNodeCount;
    }

    public double totalWeight() {
        return totalWeight;
    }

    public HugeLongArray parentArray() {
        return parent;
    }

    public long parent(long nodeId) {return parent.get(nodeId);}

    public double costToParent(long nodeId) {
        return costToParent.get(nodeId);
    }

    public void forEach(RelationshipWithPropertyConsumer consumer) {
        for (int i = 0; i < nodeCount; i++) {
            long parent = this.parent.get(i);
            double cost = this.costToParent(i);
            if (parent == -1) {
                continue;
            }
            if (!consumer.accept(parent, i, cost)) {
                return;
            }
        }
    }

    public long head(long node) {
        long p = node;
        while (-1 != parent.get(p)) {
            p = parent.get(p);
        }
        return p;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SpanningTree that = (SpanningTree) o;

        return new EqualsBuilder()
            .append(head, that.head)
            .append(nodeCount, that.nodeCount)
            .append(effectiveNodeCount, that.effectiveNodeCount)
            .append(parent, that.parent)
            .isEquals();
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(head, nodeCount, effectiveNodeCount);
        result = 31 * result + parent.hashCode();
        return result;
    }
}
