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
package org.neo4j.gds.labelpropagation;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.ConsecutiveIdsConfig;
import org.neo4j.graphalgo.config.IterationsConfig;
import org.neo4j.graphalgo.config.NodeWeightConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.SeedConfig;

public interface LabelPropagationBaseConfig extends
    AlgoBaseConfig,
    SeedConfig,
    ConsecutiveIdsConfig,
    RelationshipWeightConfig,
    NodeWeightConfig,
    IterationsConfig {

    @Value.Default
    @Override
    @Configuration.IntegerRange(min = 1)
    default int maxIterations() {
        return 10;
    }

    @Value.Check
    default void validate(){
        if (isIncremental() && consecutiveIds()) {
            throw new IllegalArgumentException("Seeding and the `consecutiveIds` option cannot be used at the same time.");
        }
    }
}
