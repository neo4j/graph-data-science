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
package org.neo4j.gds.hits;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.centrality.HitsStreamResult;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.stream.Stream;

@GdsCallable(
    name = "gds.hits.stream",
    aliases = {"gds.alpha.hits.stream"},
    description = Constants.HITS_DESCRIPTION,
    executionMode = ExecutionMode.STREAM
)
public class HitsStreamSpec implements AlgorithmSpec<Hits, PregelResult, HitsConfig, Stream<HitsStreamResult>, HitsStreamSpec.Factory> {

    @Override
    public String name() {
        return "HitsStream";
    }

    @Override
    public Factory algorithmFactory(ExecutionContext executionContext) {
        return new Factory();
    }

    @Override
    public NewConfigFunction<HitsConfig> newConfigFunction() {
        return (___,config) -> HitsConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Hits, PregelResult, HitsConfig, Stream<HitsStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }


   static  class Factory extends GraphAlgorithmFactory<Hits,HitsConfig> {

        @Override
        public Hits build(
            Graph graphOrGraphStore,
            HitsConfig configuration,
            ProgressTracker progressTracker
        ) {
            return null;
        }

        @Override
        public String taskName() {
            return "";
        }

        @Override
        public MemoryEstimation memoryEstimation(HitsConfig config) {
            return  new HitsMemoryEstimateDefinition().memoryEstimation();
        }
    }
}
