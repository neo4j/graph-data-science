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
package org.neo4j.gds.algorithms.community.specificfields;

public class K1ColoringSpecificFields {
    private final long nodeCount;
    private final long colorCount;
    private final long ranIterations;
    private final boolean didConverge;

    public static K1ColoringSpecificFields EMPTY= new K1ColoringSpecificFields(0,0,0,false);

    public K1ColoringSpecificFields(
        long nodeCount,
        long colorCount,
        long ranIterations,
        boolean didConverge
    ) {
        this.nodeCount=nodeCount;
        this.colorCount=colorCount;
        this.ranIterations=ranIterations;
        this.didConverge=didConverge;
    }

    public long nodeCount() {
        return nodeCount;
    }

    public long colorCount() {
        return colorCount;
    }

    public long ranIterations() {
        return ranIterations;
    }

    public  boolean didConverge(){
        return didConverge;
    }

}
