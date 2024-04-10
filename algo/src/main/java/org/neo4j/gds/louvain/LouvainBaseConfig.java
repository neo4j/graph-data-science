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

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.config.ToleranceConfig;

public interface LouvainBaseConfig extends
    AlgoBaseConfig,
    SeedConfig,
    ConsecutiveIdsConfig,
    RelationshipWeightConfig,
    ToleranceConfig,
    IterationsConfig {

    @Override
    @Configuration.DoubleRange(min = 0D)
    default double tolerance() {
        return 0.0001;
    }

    @Override
    @Configuration.IntegerRange(min = 1)
    default int maxIterations() {
        return 10;
    }

    default int maxLevels() {
        return 10;
    }

    default boolean includeIntermediateCommunities() {
        return false;
    }

    @Configuration.Check
    default void validate() {
        if (includeIntermediateCommunities() && consecutiveIds()) {
            throw new IllegalArgumentException("`includeIntermediateResults` and the `consecutiveIds` option cannot be used at the same time.");
        }
    }

    @Configuration.Ignore
    default LouvainParameters toParameters() {
        return new LouvainParameters(
            typedConcurrency(),
            maxIterations(),
            tolerance(),
            maxLevels(),
            includeIntermediateCommunities(),
            seedProperty()
        );
    }

    @Configuration.Ignore
    default LouvainMemoryEstimationParameters toMemoryEstimationParameters() {
        return new LouvainMemoryEstimationParameters(maxLevels(), includeIntermediateCommunities());
    }
}
