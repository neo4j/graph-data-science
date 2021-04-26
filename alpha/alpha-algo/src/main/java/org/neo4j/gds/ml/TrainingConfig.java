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
package org.neo4j.gds.ml;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.config.ConcurrencyConfig;

public interface TrainingConfig extends ConcurrencyConfig {

    int MAX_EPOCHS = 100;

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int batchSize() {
        return 100;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int minEpochs() {
        return 1;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 0)
    default int patience() {
        return 1;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int maxEpochs() {
        return MAX_EPOCHS;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0)
    default double tolerance() {
        return 1e-3;
    }

    @Value.Default
    default boolean sharedUpdater() {
        return false;
    }
}
