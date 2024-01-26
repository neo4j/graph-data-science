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
package org.neo4j.gds.degree;

import org.neo4j.gds.Orientation;

public final class DegreeCentralityParameters {
    static DegreeCentralityParameters create(
        int concurrency,
        Orientation orientation,
        boolean hasRelationshipWeightProperty,
        int minBatchSize
    ) {
        return new DegreeCentralityParameters(concurrency, orientation, hasRelationshipWeightProperty, minBatchSize);
    }

    private final int concurrency;
    private final Orientation orientation;
    private final boolean hasRelationshipWeightProperty;
    private final int minBatchSize;

    private DegreeCentralityParameters(
        int concurrency,
        Orientation orientation,
        boolean hasRelationshipWeightProperty,
        int minBatchSize
    ) {

        this.concurrency = concurrency;
        this.orientation = orientation;
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
        this.minBatchSize = minBatchSize;
    }

    int concurrency() {
        return concurrency;
    }

    Orientation orientation() {
        return orientation;
    }

    boolean hasRelationshipWeightProperty() {
        return hasRelationshipWeightProperty;
    }

    int minBatchSize() {
        return minBatchSize;
    }
}
