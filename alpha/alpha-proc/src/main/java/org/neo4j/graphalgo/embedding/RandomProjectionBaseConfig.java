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
package org.neo4j.graphalgo.embedding;

import org.immutables.value.Value;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.IterationsConfig;

import java.util.Collections;
import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface RandomProjectionBaseConfig extends AlgoBaseConfig, IterationsConfig
{

    int embeddingDimension();

    @Value.Default
    default int sparsity() {
        return 3;
    }

    @Value.Default
    default List<Double> iterationWeights() {
        return Collections.emptyList();
    }

    @Value.Default
    default float normalizationStrength() {
        return 0.0f;
    }

    @Value.Default
    default boolean normalizeL2() {
        return false;
    }

    @Value.Default
    default int seed() {
        return 0;
    }

    @Value.Check
    default void validate() {
        if (!iterationWeights().isEmpty()) {
            if (iterationWeights().size() != maxIterations()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Parameter `iterationWeights` should have `maxIterations` entries, but was %s, %s.",
                    iterationWeights().size(),
                    maxIterations()
                ));
            }
        }
    }
}