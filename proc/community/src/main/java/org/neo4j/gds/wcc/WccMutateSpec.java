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

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.NewConfigFunction;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.pipeline.AlgorithmSpec;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.logging.Log;

import java.util.stream.Stream;

class WccMutateSpec implements AlgorithmSpec<Wcc, DisjointSetStruct, WccMutateConfig, Stream<WccMutateProc.MutateResult>, WccAlgorithmFactory<WccMutateConfig>> {

    private final ProcedureCallContext callContext;
    private final AllocationTracker allocationTracker;
    private final Log log;

    WccMutateSpec(
        ProcedureCallContext callContext,
        AllocationTracker allocationTracker,
        Log log
    ) {
        this.callContext = callContext;
        this.allocationTracker = allocationTracker;
        this.log = log;
    }

    @Override
    public WccAlgorithmFactory<WccMutateConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccMutateConfig> newConfigFunction() {
        return (__, config) -> WccMutateConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccMutateConfig, Stream<WccMutateProc.MutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(WccProc::nodeProperties, this::resultBuilder, log, allocationTracker);
    }

    private AbstractCommunityResultBuilder<WccMutateProc.MutateResult> resultBuilder(AlgoBaseProc.ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computationResult) {
        return WccProc.resultBuilder(
            new WccMutateProc.MutateResult.Builder(
                callContext,
                computationResult.config().concurrency(),
                allocationTracker
            ),
            computationResult
        );
    }

}
