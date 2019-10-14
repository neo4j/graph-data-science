/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.infomap;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.RelationshipProperties;
import org.neo4j.graphdb.Direction;

/**
 * Normalized RelationshipWeights which always returns 1 / degree(source). Returns a
 * weight where - like in an probability matrix - all weights of a node add up to one.
 */
public class DegreeNormalizedRelationshipProperties implements RelationshipProperties {

    private final Degrees degrees;
    private final Direction direction;

    public DegreeNormalizedRelationshipProperties(Degrees degrees) {
        this(degrees, Direction.OUTGOING);
    }

    public DegreeNormalizedRelationshipProperties(Degrees degrees, Direction direction) {
        this.degrees = degrees;
        this.direction = direction;
    }

    @Override
    public double relationshipValue(long sourceNodeId, long targetNodeId, double fallbackValue) {
        return 1. / degrees.degree(sourceNodeId, direction);
    }
}
