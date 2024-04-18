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
package org.neo4j.gds.wcc;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.community.wcc.WccStatsResult;

import java.util.stream.Stream;

@GdsCallable(name = "gds.wcc.stats", description = WccSpecification.WCC_DESCRIPTION, executionMode = ExecutionMode.STATS)
public class WccStatsSpecification implements AlgorithmSpec<Wcc, DisjointSetStruct, WccStatsConfig, Stream<WccStatsResult>, WccAlgorithmFactory<WccStatsConfig>> {

    @Override
    public String name() {
        return "WccStats";
    }

    @Override
    public WccAlgorithmFactory<WccStatsConfig> algorithmFactory(ExecutionContext executionContext) {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccStatsConfig> newConfigFunction() {
        return (__, userInput) -> WccStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccStatsConfig, Stream<WccStatsResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

}
