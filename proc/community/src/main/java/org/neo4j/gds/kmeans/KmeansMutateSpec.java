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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.community.KmeansMutateResult;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.Kmeans.KMEANS_DESCRIPTION;

@GdsCallable(name = "gds.kmeans.mutate", aliases = {"gds.beta.kmeans.mutate"}, description = KMEANS_DESCRIPTION, executionMode = ExecutionMode.MUTATE_NODE_PROPERTY)
public class KmeansMutateSpec implements AlgorithmSpec<Kmeans, KmeansResult, KmeansMutateConfig, Stream<KmeansMutateResult>, KmeansAlgorithmFactory<KmeansMutateConfig>> {

    @Override
    public String name() {
        return "KmeansMutate";
    }

    @Override
    public KmeansAlgorithmFactory<KmeansMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KmeansAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KmeansMutateConfig> newConfigFunction() {
        return (__, config) -> KmeansMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansMutateConfig, Stream<KmeansMutateResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
