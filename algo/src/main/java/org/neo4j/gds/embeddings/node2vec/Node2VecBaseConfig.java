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
package org.neo4j.gds.embeddings.node2vec;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.EmbeddingDimensionConfig;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public interface Node2VecBaseConfig extends AlgoBaseConfig, EmbeddingDimensionConfig, RandomWalkBaseConfig {

    enum EmbeddingInitializer {
        UNIFORM,
        NORMALIZED;

        private static final List<String> VALUES = Arrays
            .stream(Node2VecBaseConfig.EmbeddingInitializer.values())
            .map(Node2VecBaseConfig.EmbeddingInitializer::name)
            .collect(Collectors.toList());
        public static Node2VecBaseConfig.EmbeddingInitializer parse(Object input) {
            if (input instanceof String) {
                var inputString = toUpperCaseWithLocale((String) input);

                if (!VALUES.contains(inputString)) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "EmbeddingInitializer `%s` is not supported. Must be one of: %s.",
                        input,
                        StringJoining.join(VALUES)
                    ));
                }

                return valueOf(toUpperCaseWithLocale(inputString));
            } else if (input instanceof Node2VecBaseConfig.EmbeddingInitializer) {
                return (Node2VecBaseConfig.EmbeddingInitializer) input;
            }

            throw new IllegalArgumentException(formatWithLocale(
                "Expected EmbeddingInitializer or String. Got %s.",
                input.getClass().getSimpleName()
            ));
        }

        public static String toString(Node2VecBaseConfig.EmbeddingInitializer embeddingInitializer) {
            return embeddingInitializer.toString();
        }
    }

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int windowSize() {
        return 10;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int negativeSamplingRate() {
        return 5;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.00001, minInclusive = false, max=1.0)
    default double positiveSamplingFactor() {
        return 0.001;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.00001, minInclusive = false, max=1.0)
    default double negativeSamplingExponent() {
        return 0.75;
    }

    @Override
    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int embeddingDimension() {
        return 128;
    }

    @Configuration.ConvertWith("org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig.EmbeddingInitializer#parse")
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.node2vec.Node2VecBaseConfig.EmbeddingInitializer#toString")
    default EmbeddingInitializer embeddingInitializer() {
        return EmbeddingInitializer.NORMALIZED;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double initialLearningRate() {
        return 0.025;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double minLearningRate() {
        return 0.0001;
    }

    @Value.Default
    default int iterations() {
        return 1;
    }

    @Configuration.Ignore
    @Value.Default
    @Override
    default List<Long> sourceNodes() {
        return List.of();
    }
}
