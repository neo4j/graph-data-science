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
package org.neo4j.gds.modularityoptimization;

import org.immutables.value.Value;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.config.ToleranceConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;

public interface ModularityOptimizationConfig extends
    AlgoBaseConfig,
    IterationsConfig,
    SeedConfig,
    ConsecutiveIdsConfig,
    ToleranceConfig,
    RelationshipWeightConfig {

    int DEFAULT_ITERATIONS = 10;

    @Value.Default
    @Override
    default double tolerance() {
        return 0.0001;
    }

    @Override
    @Value.Default
    default int maxIterations() {
        return DEFAULT_ITERATIONS;
    }

    @Value.Default
    default int batchSize() {
        return ParallelUtil.DEFAULT_BATCH_SIZE;
    }
}
