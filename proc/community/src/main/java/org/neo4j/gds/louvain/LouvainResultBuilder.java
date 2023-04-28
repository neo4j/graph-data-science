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
package org.neo4j.gds.louvain;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;

abstract class LouvainResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {

    long levels = -1;
    double[] modularities = new double[]{};
    double modularity = -1;

    LouvainResultBuilder(ProcedureReturnColumns returnColumns, int concurrency) {
        super(returnColumns, concurrency);
    }

    @NotNull
    static LouvainResultBuilder<MutateResult> createForMutate(
        ComputationResult<Louvain, LouvainResult, LouvainMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        LouvainResultBuilder<MutateResult> procResultBuilder = new LouvainMutateResultBuilder(
            executionContext.returnColumns(),
            computeResult.config().concurrency()
        );
        computeResult.result().ifPresent(result -> {
            procResultBuilder
                .withLevels(result.ranLevels())
                .withModularity(result.modularity())
                .withModularities(result.modularities())
                .withCommunityFunction(result::getCommunity);
        });

        return procResultBuilder;
    }

    static LouvainResultBuilder<WriteResult> createForWrite(
        ComputationResult<Louvain, LouvainResult, LouvainWriteConfig> computeResult,
        ExecutionContext executionContext
    ) {
        LouvainResultBuilder<WriteResult> procResultBuilder = new LouvainWriteResultsBuilder(
            executionContext.returnColumns(),
            computeResult.config().concurrency()
        );
        computeResult.result().ifPresent(result -> {
            procResultBuilder
                .withLevels(result.ranLevels())
                .withModularity(result.modularity())
                .withModularities(result.modularities())
                .withCommunityFunction(result::getCommunity);
        });

        return procResultBuilder;
    }

    LouvainResultBuilder<PROC_RESULT> withLevels(long levels) {
        this.levels = levels;
        return this;
    }

    LouvainResultBuilder<PROC_RESULT> withModularities(double[] modularities) {
        this.modularities = modularities;
        return this;
    }

    LouvainResultBuilder<PROC_RESULT> withModularity(double modularity) {
        this.modularity = modularity;
        return this;
    }
}
