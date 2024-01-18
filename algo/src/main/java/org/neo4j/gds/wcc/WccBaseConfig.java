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
package org.neo4j.gds.wcc;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConsecutiveIdsConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.SeedConfig;

public interface WccBaseConfig extends AlgoBaseConfig, SeedConfig, ConsecutiveIdsConfig, RelationshipWeightConfig {

    default double threshold() {
        return 0D;
    }

    @Configuration.Ignore
    default boolean hasThreshold() {
        return !Double.isNaN(threshold()) && threshold() > 0;
    }

    @Value.Check
    default void validate() {
        if (threshold() > 0 && relationshipWeightProperty().isEmpty()) {
            throw new IllegalArgumentException("Specifying a threshold requires `relationshipWeightProperty` to be set.");
        }
    }

    @Configuration.Ignore
    default WccParameters toParameters() {
        return WccParameters.create(threshold(), seedProperty(), concurrency());
    }
}
