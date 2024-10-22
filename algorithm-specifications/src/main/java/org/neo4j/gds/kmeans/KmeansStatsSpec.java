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
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.algorithms.community.KmeansStatsResult;

import java.util.stream.Stream;

import static org.neo4j.gds.kmeans.Kmeans.KMEANS_DESCRIPTION;

@GdsCallable(name = "gds.kmeans.stats", aliases = {"gds.beta.kmeans.stats"}, description = KMEANS_DESCRIPTION, executionMode = ExecutionMode.STATS)
public class KmeansStatsSpec implements AlgorithmSpec<Kmeans, KmeansResult, KmeansStatsConfig, Stream<KmeansStatsResult>, KmeansAlgorithmFactory<KmeansStatsConfig>> {

    @Override
    public String name() {
        return "KmeansStats";
    }

    @Override
    public KmeansAlgorithmFactory<KmeansStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new KmeansAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<KmeansStatsConfig> newConfigFunction() {
        return (__, config) -> KmeansStatsConfig.of(config);
    }


    @Override
    public ComputationResultConsumer<Kmeans, KmeansResult, KmeansStatsConfig, Stream<KmeansStatsResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }
}
