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
package org.neo4j.gds.maxflow;

public class IgnoreNodeConstraints implements NodeConstraintsIdMap {

    @Override
    public long mapNode(long nodeId) {
        return nodeId;
    }

    @Override
    public boolean isFakeNode(long nodeId) {
        return false;
    }

    @Override
    public boolean hasCapacityConstraint(long nodeId) {
        return false;
    }

    @Override
    public double relationshipCapacity(long relationshipId) {return 0;}

    @Override
    public long toFakeNodeOf(long nodeId) {
        return 0;
    }

    @Override
    public long realNodeOf(long nodeId) {
        return 0;
    }


    @Override
    public long numberOfCapacityNodes() {
        return 0;
    }

    @Override
    public long capacityRelId(long nodeId) {
        return 0;
    }
}
