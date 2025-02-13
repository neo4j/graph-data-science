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

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;

import java.util.function.Function;

final class ClusterHierarchy {
    private final long root;
    private final HugeLongArray left;
    private final HugeLongArray right;
    private final HugeDoubleArray lambda;
    private final HugeLongArray size;
    private final long nodeCount;

    private ClusterHierarchy(
        long root, HugeLongArray left,
        HugeLongArray right,
        HugeDoubleArray lambda,
        HugeLongArray size,
        long nodeCount
    ) {
        this.root = root;
        this.left = left;
        this.right = right;
        this.lambda = lambda;
        this.size = size;
        this.nodeCount = nodeCount;
    }

    static ClusterHierarchy create(long nodeCount, HugeObjectArray<Edge> edges) {
        var left = HugeLongArray.newArray(nodeCount);
        var right = HugeLongArray.newArray(nodeCount);
        var lambda = HugeDoubleArray.newArray(nodeCount);
        var size = HugeLongArray.newArray(nodeCount);

        var unionFind = new ClusterHierarchyUnionFind(nodeCount);

        long currentRoot = -1L;

        var sizeFn = (Function<Long, Long>) n -> n < nodeCount ? 1L : size.get(n - nodeCount);

        for (var i = 0; i < edges.size(); i++) {
            var edge = edges.get(i);
            var l = unionFind.find(edge.source());
            var r = unionFind.find(edge.target());

            currentRoot = unionFind.union(l, r);
            var adaptedIndex = currentRoot - nodeCount;
            left.set(adaptedIndex, l);
            right.set(adaptedIndex, r);
            lambda.set(adaptedIndex, edge.distance());

            var leftSize = sizeFn.apply(l);
            var rigthSize = sizeFn.apply(r);

            size.set(adaptedIndex, leftSize + rigthSize);
        }

        return new ClusterHierarchy(currentRoot, left, right, lambda, size, nodeCount);
    }

    long root() {
        return this.root;
    }

    long left(long node) {
        return left.get(node - nodeCount);
    }

    long right(long node) {
        return right.get(node - nodeCount);
    }

    long size(long node) {
        return node < nodeCount
            ? 1L
            : size.get(node - nodeCount);
    }

    double lambda(long node) {
        return lambda.get(node - nodeCount);
    }

}
