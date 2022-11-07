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
package org.neo4j.gds.embeddings.hashgnn;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;

import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@Configuration
public interface FeatureBinarizationConfig {
    @Configuration.IntegerRange(min = 1)
    int dimension();
    @Configuration.IntegerRange(min = 1)
    int densityLevel();

    @Value.Check
    default void validate() {
        if (2 * densityLevel() > dimension()) {
            throw new IllegalArgumentException(formatWithLocale("The value %d of `densityLevel` may not exceed half of the value %d of `dimension`.", densityLevel(), dimension()));
        }
    }

    @Configuration.ToMap
    @Value.Auxiliary
    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(); // Will be overwritten
    }
}
