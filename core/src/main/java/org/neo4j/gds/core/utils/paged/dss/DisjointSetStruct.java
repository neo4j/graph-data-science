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
package org.neo4j.gds.core.utils.paged.dss;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;

/**
 * Disjoint-set-struct is a data structure that keeps track of a set
 * of elements partitioned into a number of disjoint (non-overlapping) subsets.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Disjoint-set_data_structure">Wiki</a>
 */
public interface DisjointSetStruct {

    /**
     * Joins the set of p (Sp) with set of q (Sq).
     *
     * @param p an item of Sp
     * @param q an item of Sq
     */
    void union(long p, long q);

    /**
     * Find set Id of element p.
     *
     * @param nodeId the element in the set we are looking for
     * @return an id of the set it belongs to
     */
    long setIdOf(long nodeId);

    /**
     * Check if p and q belong to the same set.
     * use only in tests.
     *
     * @param p a set item
     * @param q a set item
     * @return true if both items belong to the same set, false otherwise
     */
    @TestOnly
    boolean sameSet(long p, long q);

    /**
     * Number of elements stored in the data structure.
     *
     * @return element count
     */
    long size();

    /**
     * Wraps the DisjointSetStruct in an LongNodeProperties interface
     *
     * @return wrapped DisjointSetStruct
     */
    default LongNodePropertyValues asNodeProperties() {
        return new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return setIdOf(nodeId);
            }

            @Override
            public long nodeCount() {
                return DisjointSetStruct.this.size();
            }
        };
    }
}
