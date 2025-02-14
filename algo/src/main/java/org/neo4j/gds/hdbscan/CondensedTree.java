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

class CondensedTree {

    private final long root;
    private final HugeLongArray parent;
    private final HugeDoubleArray lambda;
    private final long maximumClusterId;

    CondensedTree(long root, HugeLongArray parent, HugeDoubleArray lambda, long maximumClusterId) {
        this.root = root;
        this.parent = parent;
        this.lambda = lambda;
        this.maximumClusterId = maximumClusterId;
    }

    long root() {
        return root;
    }

    long parent(long node) {
        return parent.get(node);
    }

    long fellOutOf(long node) {
        return parent.get(node);
    }

    long maximumClusterId() {
        return maximumClusterId;
    }

    double lambda(long node) {
        return lambda.get(node);
    }
}
