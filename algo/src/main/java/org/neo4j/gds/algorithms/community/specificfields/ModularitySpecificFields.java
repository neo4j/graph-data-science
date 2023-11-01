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

public final class ModularitySpecificFields {


    public static final ModularitySpecificFields EMPTY =
        new ModularitySpecificFields(0,  0L, 0L, 0.0d);

    private final long nodeCount;
    private final long relationshipCount;
    private final long communityCount;
    private final double modularity;

    public ModularitySpecificFields(
        long nodeCount,
        long relationshipCount,
        long communityCount,
        double modularity
    ) {
        this.communityCount = communityCount;
        this.nodeCount = nodeCount;
        this.relationshipCount=relationshipCount;
        this.modularity=modularity;
    }

    public long communityCount() {
        return communityCount;
    }

    public long nodeCount() {
        return nodeCount;
    }

    public long  relationshipCount() {
        return relationshipCount;
    }

    public double modularity(){ return  modularity;}
}
