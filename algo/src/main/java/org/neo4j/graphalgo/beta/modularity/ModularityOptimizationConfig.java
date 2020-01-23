/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.beta.modularity;

import org.immutables.value.Value;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.IterationsConfig;
import org.neo4j.graphalgo.newapi.RelationshipWeightConfig;
import org.neo4j.graphalgo.newapi.SeedConfig;
import org.neo4j.graphalgo.newapi.ToleranceConfig;

public interface ModularityOptimizationConfig extends
    AlgoBaseConfig,
    IterationsConfig,
    SeedConfig,
    ToleranceConfig,
    RelationshipWeightConfig {

    int DEFAULT_ITERATIONS = 10;

    @Value.Default
    @Override
    default double tolerance() {
        return 1E-7;
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
