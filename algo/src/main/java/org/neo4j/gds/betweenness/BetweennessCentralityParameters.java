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
package org.neo4j.gds.betweenness;

import java.util.Optional;

public final class BetweennessCentralityParameters {

    public static BetweennessCentralityParameters create(
        int concurrency,
        Optional<Long> samplingSize,
        Optional<Long> samplingSeed,
        boolean hasRelationshipWeightProperty
    ) {
        return new BetweennessCentralityParameters(
            concurrency,
            samplingSize,
            samplingSeed,
            hasRelationshipWeightProperty
        );
    }

    private final int concurrency;
    private final Optional<Long> samplingSize;
    private final Optional<Long> samplingSeed;
    private final boolean hasRelationshipWeightProperty;

    private BetweennessCentralityParameters(
        int concurrency,
        Optional<Long> samplingSize,
        Optional<Long> samplingSeed,
        boolean hasRelationshipWeightProperty
    ) {
        this.concurrency = concurrency;
        this.samplingSize = samplingSize;
        this.samplingSeed = samplingSeed;
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
    }

    int concurrency() {
        return concurrency;
    }

    public Optional<Long> samplingSize() {
        return samplingSize;
    }

    public Optional<Long> samplingSeed() {
        return samplingSeed;
    }

    boolean hasRelationshipWeightProperty() {
        return hasRelationshipWeightProperty;
    }
}
