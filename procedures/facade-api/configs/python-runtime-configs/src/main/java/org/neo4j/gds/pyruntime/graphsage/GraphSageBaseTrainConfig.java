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
package org.neo4j.gds.pyruntime.graphsage;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.config.RandomSeedConfig;
import org.neo4j.gds.pyruntime.BaseTrainGraphInput;

import java.util.List;

/**
 * Shared GraphSage base config for training. GraphSage trains as a remote python-runtime job.
 * These parameter definitions are mirrored from
 * {@code data_science_python_runtime.config.graphsage_config} and are shared between the
 * python-runtime compute configs (flight-server) and the Cypher-surface configs (session)
 * so the two never drift.
 */
public interface GraphSageBaseTrainConfig extends BaseTrainGraphInput, RandomSeedConfig, JobIdConfig {

    String modelName();

    @Configuration.IntegerRange(min = 0, minInclusive = false)
    int epochs();

    @Configuration.ConvertWith(
        method = "org.neo4j.gds.pyruntime.graphsage.GraphSageActivationFunction#parse")
    @Configuration.ToMapValue("org.neo4j.gds.pyruntime.graphsage.GraphSageActivationFunction#toMapValue")
    default GraphSageActivationFunction activationFunction() {
        return GraphSageActivationFunction.RELU;
    }

    @Configuration.ConvertWith(method = "org.neo4j.gds.pyruntime.graphsage.GraphSageAggregator#parse")
    @Configuration.ToMapValue("org.neo4j.gds.pyruntime.graphsage.GraphSageAggregator#toMapValue")
    default GraphSageAggregator aggregator() {
        return GraphSageAggregator.MEAN;
    }

    @Configuration.IntegerRange(min = 0, minInclusive = false)
    default int embeddingDimension() {
        return 256;
    }

    @Configuration.DoubleRange(min = 0.0, minInclusive = false)
    default double learningRate() {
        return 0.001;
    }

    @Configuration.IntegerRange(min = 1)
    @Configuration.ConvertWith(method = "convertToIntNeighbors")
    default List<Integer> numNeighbors() {
        return List.of(20, 10);
    }

    static List<Integer> convertToIntNeighbors(List<? extends Number> input) {
        try {
            return input.stream()
                .map(Number::longValue)
                .map(Math::toIntExact)
                .toList();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("`numNeighbors` entries must be smaller than 2^31", e);
        }
    }

    @Configuration.DoubleRange(min = 0.0, max = 1.0, maxInclusive = false)
    default double dropout() {
        return 0.1;
    }

    default boolean layerNormalization() {
        return true;
    }

    @Configuration.Check
    default void validateFeatureProperties() {
        if (featureProperties().isEmpty()) {
            throw new IllegalArgumentException("`featureProperties` must not be empty");
        }
    }

    @Configuration.Check
    default void validateNumNeighbors() {
        if (numNeighbors().isEmpty()) {
            throw new IllegalArgumentException("`numNeighbors` must not be empty");
        }
    }
}
