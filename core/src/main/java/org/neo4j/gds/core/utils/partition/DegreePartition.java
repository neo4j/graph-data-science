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
package org.neo4j.gds.core.utils.partition;

import java.util.Objects;

public class DegreePartition extends Partition {

    private final long relationshipCount;

    public DegreePartition(long startNode, long nodeCount, long relationshipCount) {
        super(startNode, nodeCount);
        this.relationshipCount = relationshipCount;
    }

    public long relationshipCount() {
        return relationshipCount;
    }

    public static DegreePartition of(long startNode, long nodeCount, long totalDegree) {
        return new DegreePartition(startNode, nodeCount, totalDegree);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        DegreePartition that = (DegreePartition) o;
        return relationshipCount == that.relationshipCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relationshipCount);
    }

    @Override
    public String toString() {
        return "DegreePartition{" +
            "start:" + this.startNode() +
            ", length:" + this.nodeCount() +
            ", relationshipCount=" + relationshipCount +
            '}';
    }
}
