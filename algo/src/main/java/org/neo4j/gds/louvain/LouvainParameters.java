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
package org.neo4j.gds.louvain;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.Parameters;

@Parameters
public final class LouvainParameters {
    public static LouvainParameters create(
        int concurrency,
        int maxIterations,
        double tolerance,
        int maxLevels,
        boolean includeIntermediateCommunities,
        @Nullable String seedProperty
    ) {
        return new LouvainParameters(
            concurrency,
            maxIterations,
            tolerance,
            maxLevels,
            includeIntermediateCommunities,
            seedProperty
        );
    }

    private final int concurrency;
    private final int maxIterations;
    private final double tolerance;
    private final int maxLevels;
    private final boolean includeIntermediateCommunities;
    private final String seedProperty;

    private LouvainParameters(
        int concurrency,
        int maxIterations,
        double tolerance,
        int maxLevels,
        boolean includeIntermediateCommunities,
        @Nullable String seedProperty
    ) {
        this.concurrency = concurrency;
        this.maxIterations = maxIterations;
        this.tolerance = tolerance;
        this.maxLevels = maxLevels;
        this.includeIntermediateCommunities = includeIntermediateCommunities;
        this.seedProperty = seedProperty;
    }

    int concurrency() {
        return concurrency;
    }

    int maxIterations() {
        return maxIterations;
    }

    double tolerance() {
        return tolerance;
    }

    int maxLevels() {
        return maxLevels;
    }

    boolean includeIntermediateCommunities() {
        return includeIntermediateCommunities;
    }

    String seedProperty() {
        return seedProperty;
    }
}
