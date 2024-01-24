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
package org.neo4j.gds.leiden;

import org.jetbrains.annotations.Nullable;

import java.util.Optional;

public final class LeidenParameters {

    static LeidenParameters create(
        int concurrency,
        double tolerance,
        @Nullable String seedProperty,
        int maxLevels,
        double gamma,
        double theta,
        boolean includeIntermediateCommunities,
        Optional<Long> randomSeed
    ) {
        return new LeidenParameters(
            concurrency,
            tolerance,
            seedProperty,
            maxLevels,
            gamma,
            theta,
            includeIntermediateCommunities,
            randomSeed
        );
    }

    private final int concurrency;
    private final double tolerance;
    private final String seedProperty;
    private final int maxLevels;
    private final double gamma;
    private final double theta;
    private final boolean includeIntermediateCommunities;
    private final Optional<Long> randomSeed;

    private LeidenParameters(
        int concurrency,
        double tolerance,
        @Nullable String seedProperty,
        int maxLevels,
        double gamma,
        double theta,
        boolean includeIntermediateCommunities,
        Optional<Long> randomSeed
    ) {
        this.concurrency = concurrency;
        this.tolerance = tolerance;
        this.seedProperty = seedProperty;
        this.maxLevels = maxLevels;
        this.gamma = gamma;
        this.theta = theta;
        this.includeIntermediateCommunities = includeIntermediateCommunities;
        this.randomSeed = randomSeed;
    }

    int concurrency() {
        return concurrency;
    }

    double tolerance() {
        return tolerance;
    }

    @Nullable String seedProperty() {
        return seedProperty;
    }

    int maxLevels() {
        return maxLevels;
    }

    double gamma() {
        return gamma;
    }

    double theta() {
        return theta;
    }

    boolean includeIntermediateCommunities() {
        return includeIntermediateCommunities;
    }

    Optional<Long> randomSeed() {
        return randomSeed;
    }
}
