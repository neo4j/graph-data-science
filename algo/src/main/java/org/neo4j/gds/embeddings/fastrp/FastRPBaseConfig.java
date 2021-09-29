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
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.EmbeddingDimensionConfig;
import org.neo4j.gds.config.FeaturePropertiesConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;

import java.util.List;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@SuppressWarnings("immutables:subtype")
public interface FastRPBaseConfig extends AlgoBaseConfig, EmbeddingDimensionConfig, RelationshipWeightConfig, FeaturePropertiesConfig, RandomSeedConfig {

    List<Number> DEFAULT_ITERATION_WEIGHTS = List.of(0.0D, 1.0D, 1.0D);

    @Value.Derived
    @Configuration.Ignore
    default int propertyDimension() {
        return (int) (embeddingDimension() * propertyRatio());
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0, max = 1.0)
    default double propertyRatio() {
        return 0.0;
    }

    @Value.Default
    default List<Number> iterationWeights() {
        return DEFAULT_ITERATION_WEIGHTS;
    }

    @Value.Default
    default Number nodeSelfInfluence() {return 0;}

    @Configuration.Ignore
    @Value.Derived
    default int iterations() {
        return iterationWeights().size();
    }

    @Value.Default
    default float normalizationStrength() {
        return 0.0f;
    }

    @Value.Check
    default void validate() {
        if (iterationWeights().isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The value of `%s` must not be empty.",
                "iterationWeights"
            ));
        }
        if (propertyRatio() > 0.0) {
            if (featureProperties().isEmpty()) {
                throw new IllegalArgumentException("When `propertyRatio` is non-zero, `featureProperties` may not be empty.");
            }
        }
        // propertyRatio=0 and non-empty featureProperties is allowed because otherwise it would be harder to change propertyRatio back and forth
        for (Object weight : iterationWeights()) {
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
