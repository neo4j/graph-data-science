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
package org.neo4j.gds.embeddings.fastrp;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.EmbeddingDimensionConfig;
import org.neo4j.graphalgo.config.FeaturePropertiesConfig;
import org.neo4j.graphalgo.config.RandomSeedConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;

import java.util.List;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface FastRPBaseConfig extends AlgoBaseConfig, EmbeddingDimensionConfig, RelationshipWeightConfig, FeaturePropertiesConfig, RandomSeedConfig {

    List<Number> DEFAULT_ITERATION_WEIGHTS = List.of(0.0D, 1.0D, 1.0D);

    @Value.Default
    default int propertyDimension() {
        return 0;
    }

    @Value.Default
    default List<Number> iterationWeights() {
        return DEFAULT_ITERATION_WEIGHTS;
    }

    @Configuration.Ignore
    @Value.Derived
    default int iterations() {
        return iterationWeights().size();
    }

    @Value.Default
    default float normalizationStrength() {
        return 0.0f;
    }

    static void validateCommon(List<? extends Number> iterationWeights) {
        if (iterationWeights.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must not be empty.",
                "iterationWeights"
            ));
        }
        for (Object weight : iterationWeights) {
            if (!(weight instanceof Number)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Iteration weights must be numbers, but found `%s` of type `%s`",
                    weight,
                    weight == null ? "null" : weight.getClass().getSimpleName()
                ));
            }
        }
    }

    @Configuration.Ignore
    static ImmutableFastRPBaseConfig.Builder builder() {
        return ImmutableFastRPBaseConfig.builder();
    }
}
