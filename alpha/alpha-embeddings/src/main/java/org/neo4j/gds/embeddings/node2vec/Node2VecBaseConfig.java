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
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.EmbeddingDimensionConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;

public interface Node2VecBaseConfig extends AlgoBaseConfig, EmbeddingDimensionConfig, RelationshipWeightConfig {

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int walkLength() {
        return 80;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int walksPerNode() {
        return 10;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 2)
    default int windowSize() {
        return 10;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int walkBufferSize() {
        return 1000;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0)
    default double inOutFactor() {
        return 1.0;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.0)
    default double returnFactor() {
        return 1.0;
    }

    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int negativeSamplingRate() {
        return 5;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.00001, minInclusive = false, max=1.0)
    default double centerSamplingFactor() {
        return 0.001;
    }

    @Value.Default
    @Configuration.DoubleRange(min = 0.00001, minInclusive = false, max=1.0)
    default double contextSamplingExponent() {
        return 0.75;
    }

    @Override
    @Value.Default
    @Configuration.IntegerRange(min = 1)
    default int embeddingDimension() {
        return 128;
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

}
