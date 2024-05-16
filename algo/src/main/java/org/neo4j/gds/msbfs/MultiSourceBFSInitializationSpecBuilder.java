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
package org.neo4j.gds.msbfs;

class MultiSourceBFSInitializationSpecBuilder {

    private boolean seenNext = false;
    private boolean sortSourceNodes = false;
    private long[] sourceNodes = null;
    private boolean allowStartNodeTraversal = false;

    MultiSourceBFSInitializationSpecBuilder seenNext(boolean seenNext) {
        this.seenNext = seenNext;
        return this;
    }

    MultiSourceBFSInitializationSpecBuilder sortSourceNodes(boolean sortSourceNodes) {
        this.sortSourceNodes = sortSourceNodes;
        return this;

    }

    MultiSourceBFSInitializationSpecBuilder allowStartNodeTraversal(boolean allowStartNodeTraversal) {
        this.allowStartNodeTraversal = allowStartNodeTraversal;
        return this;
    }

    MultiSourceBFSInitializationSpecBuilder sourceNodes(long[] sourceNodes) {
        this.sourceNodes = sourceNodes;
        return this;

    }

    MultiSourceBFSInitializationSpec build() {
        return new MultiSourceBFSInitializationSpec(
            seenNext,
            sortSourceNodes,
            sourceNodes,
            allowStartNodeTraversal
        );
    }


}
