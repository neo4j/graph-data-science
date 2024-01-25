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
package org.neo4j.gds.conductance;

public final class ConductanceParameters {
    public static ConductanceParameters create(int concurrency, int minBatchSize, boolean hasRelationshipProperty, String communityProperty) {
        return new ConductanceParameters(concurrency, minBatchSize, hasRelationshipProperty, communityProperty);
    }

    private final int concurrency;
    private final int minBatchSize;
    private final boolean hasRelationshipWeightProperty;
    private final String communityProperty;

    private ConductanceParameters(
        int concurrency,
        int minBatchSize,
        boolean hasRelationshipWeightProperty,
        String communityProperty
    ) {
        this.concurrency = concurrency;
        this.minBatchSize = minBatchSize;
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
        this.communityProperty = communityProperty;
    }

    int concurrency() {
        return concurrency;
    }

    int minBatchSize() {
        return minBatchSize;
    }

    boolean hasRelationshipWeightProperty() {
        return hasRelationshipWeightProperty;
    }

    String communityProperty() {
        return communityProperty;
    }
}
