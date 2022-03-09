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
package org.neo4j.gds.paths.traverse;

public interface ExitPredicate {

    ExitPredicate FOLLOW = (s, t, w) -> Result.FOLLOW;

    enum Result {
        /**
         * add current node to the result set and visit all neighbors
         */
        FOLLOW,
        /**
         * add current node to the result set and terminate traversal
         */
        BREAK,
        /**
         * does not add node to the result set, does not follow its neighbors,
         * just continue with next element on the stack
         */
        CONTINUE
    }

    /**
     * called once for each accepted node during traversal
     *
     * @param sourceNode     the source node
     * @param currentNode    the current node
     * @param weightAtSource the total weight that has been collected by the Aggregator during the traversal
     * @return a result
     */
    Result test(long sourceNode, long currentNode, double weightAtSource);

}
