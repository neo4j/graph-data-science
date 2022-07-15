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
package org.neo4j.gds.graphsampling.config;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Optional;

@Configuration
@SuppressWarnings("immutables:subtype")
public interface RandomWalkWithRestartsConfig extends AlgoBaseConfig {

    default List<Long> startNodes() {
        return List.of();
    }
    Optional<Long> randomSeed();

    @Configuration.DoubleRange(min = 0.0, max = 1.0, minInclusive = false, maxInclusive = false)
    default double restartProbability() {
        return 0.1;
    }

    /**
     * The ratio of nodes that we wish to sample.
     */
    @Configuration.DoubleRange(min = 0.0, max = 1.0, minInclusive = false)
    default double samplingRatio() {
        return 0.15;
    }

    static RandomWalkWithRestartsConfig of(
        CypherMapWrapper procedureConfig
    ) {
        return new RandomWalkWithRestartsConfigImpl(procedureConfig);
    }
}
