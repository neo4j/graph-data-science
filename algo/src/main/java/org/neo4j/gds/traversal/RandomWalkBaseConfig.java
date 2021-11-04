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
package org.neo4j.gds.traversal;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SourceNodesConfig;

public interface RandomWalkBaseConfig extends AlgoBaseConfig, RelationshipWeightConfig, RandomSeedConfig, SourceNodesConfig {

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int walkLength() {
        return 80;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int walksPerNode() {
        return 10;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int walkBufferSize() {
        return 1000;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0)
    default double inOutFactor() {
        return 1.0;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0)
    default double returnFactor() {
        return 1.0;
    }
}
