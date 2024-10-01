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

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.EmbeddingDimensionConfig;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.List;


public interface Node2VecBaseConfig extends AlgoBaseConfig, EmbeddingDimensionConfig, RandomWalkBaseConfig {

    @Configuration.IntegerRange(min = 2)
    default int windowSize() {
        return 10;
    }

    @Configuration.IntegerRange(min = 1)
    default int negativeSamplingRate() {
        return 5;
    }

    @Configuration.DoubleRange(min = 0.00001, minInclusive = false, max=1.0)
    default double positiveSamplingFactor() {
        return 0.001;
    }

    @Configuration.DoubleRange(min = 0.00001, minInclusive = false, max=1.0)
    default double negativeSamplingExponent() {
        return 0.75;
    }

    @Override
    @Configuration.IntegerRange(min = 1)
    default int embeddingDimension() {
        return 128;
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.embeddings.node2vec.EmbeddingInitializer#parse", inverse = Configuration.ConvertWith.INVERSE_IS_TO_MAP)
    @Configuration.ToMapValue("org.neo4j.gds.embeddings.node2vec.EmbeddingInitializer#toString")
    default EmbeddingInitializer embeddingInitializer() {
        return EmbeddingInitializer.NORMALIZED;
    }

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double initialLearningRate() {
        return 0.025;
    }

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double minLearningRate() {
        return 0.0001;
    }

    default int iterations() {
        return 1;
    }

    @Configuration.Ignore
    @Override
    default List<Long> sourceNodes() {
        return List.of();
    }
}
