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
import org.neo4j.graphalgo.config.RelationshipWeightConfig;

import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ValueClass
public interface RandomProjectionBaseConfig extends AlgoBaseConfig, RelationshipWeightConfig {

    String ITERATION_WEIGHTS_KEY = "iterationWeights";

    List<Double> DEFAULT_ITERATION_WEIGHTS = List.of(0.0D, 0.0D, 1.0D);

    int embeddingSize();

    @Value.Default
    default int sparsity() {
        return 3;
    }

    @Configuration.Key(ITERATION_WEIGHTS_KEY)
    @Value.Default
    default List<Double> iterationWeights() {
        return DEFAULT_ITERATION_WEIGHTS;
    }

    @Configuration.Ignore
    @Value.Derived
    default int maxIterations() {
        return iterationWeights().size();
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
        if (iterationWeights().isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must not be empty.",
                ITERATION_WEIGHTS_KEY
            ));
        }
    }
}
