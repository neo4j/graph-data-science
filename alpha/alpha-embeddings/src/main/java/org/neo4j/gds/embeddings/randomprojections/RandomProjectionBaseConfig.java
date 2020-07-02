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
package org.neo4j.gds.embeddings.randomprojections;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.IterationsConfig;

import java.util.Collections;
import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ValueClass
public interface RandomProjectionBaseConfig extends AlgoBaseConfig, IterationsConfig {

    String ITERATION_WEIGHTS_KEY = "iterationWeights";

    int embeddingSize();

    @Value.Default
    default int sparsity() {
        return 3;
    }

    @Configuration.Key(ITERATION_WEIGHTS_KEY)
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

    @Value.Check
    default void validate() {
        if (!iterationWeights().isEmpty()) {
            var numberOfIterationWeights = iterationWeights().size();
            if (numberOfIterationWeights != maxIterations()) {
                var message = formatWithLocale(
                    "The value of `%1$s` must be a list where its length is the " +
                    "same value as the configured value for `%2$s`.%n" +
                    "`%2$s` is defined as `%3$d` but `%1$s` contains `%4$d` %5$s.",
                    ITERATION_WEIGHTS_KEY,
                    MAX_ITERATIONS_KEY,
                    maxIterations(),
                    numberOfIterationWeights,
                    numberOfIterationWeights == 1 ? "entry" : "entries"
                );
                throw new IllegalArgumentException(message);
            }
        }
    }
}
