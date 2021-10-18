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
package org.neo4j.gds.config;

import org.immutables.value.Value;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface SingleThreadedRandomSeedConfig extends ConcurrencyConfig, RandomSeedConfig {
    @Value.Check
    default void validate() {
        randomSeed().ifPresent(unused -> {
            if (concurrency() > 1) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Configuration parameter 'randomSeed' may only be set if parameter 'concurrency' is equal to 1, but got %d.",
                    concurrency()
                ));
            }
        });
    }
}
