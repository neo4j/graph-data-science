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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.annotation.Parameters;

import java.util.List;
import java.util.Optional;

@Parameters
public final class ApproxMaxKCutParameters {
    private final byte k;
    private final int iterations;
    private final int vnsMaxNeighborhoodOrder;
    private final int concurrency;
    private final int minBatchSize;
    private final Optional<Long> randomSeed;
    private final List<Long> minCommunitySizes;
    private final boolean hasRelationshipWeightProperty;
    private final boolean minimize;

    public ApproxMaxKCutParameters(
        byte k,
        int iterations,
        int vnsMaxNeighborhoodOrder,
        int concurrency,
        int minBatchSize,
        Optional<Long> randomSeed,
        List<Long> minCommunitySizes,
        boolean hasRelationshipWeightProperty,
        boolean minimize
    ) {
        this.k = k;
        this.iterations = iterations;
        this.vnsMaxNeighborhoodOrder = vnsMaxNeighborhoodOrder;
        this.concurrency = concurrency;
        this.minBatchSize = minBatchSize;
        this.randomSeed = randomSeed;
        this.minCommunitySizes = minCommunitySizes;
        this.hasRelationshipWeightProperty = hasRelationshipWeightProperty;
        this.minimize = minimize;
    }

    byte k() {
        return this.k;
    }

    int iterations() {
        return this.iterations;
    }

    int vnsMaxNeighborhoodOrder() {
        return this.vnsMaxNeighborhoodOrder;
    }

    int concurrency() {
        return this.concurrency;
    }

    int minBatchSize() {
        return this.minBatchSize;
    }

    Optional<Long> randomSeed() {
        return this.randomSeed;
    }

    List<Long> minCommunitySizes() {
        return this.minCommunitySizes;
    }

    boolean hasRelationshipWeightProperty() {
        return this.hasRelationshipWeightProperty;
    }

    boolean minimize() {
        return this.minimize;
    }
}
