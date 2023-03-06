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
package org.neo4j.gds.modularityoptimization;

import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.nodeproperties.ConsecutiveLongNodePropertyValues;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

final class ModularityOptimizationProc {
    static final String MODULARITY_OPTIMIZATION_DESCRIPTION = "The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.";

    static <PROC_RESULT, CONFIG extends ModularityOptimizationConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        ModularityOptimizationResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<ModularityOptimization, ModularityOptimization, CONFIG> computeResult
    ) {
        ModularityOptimization result = computeResult.result();

        return procResultBuilder
            .withModularity(result.getModularity())
            .withRanIterations(result.getIterations())
            .didConverge(result.didConverge())
            .withCommunityFunction(result::getCommunityId)
            .withConfig(computeResult.config());

    }

    static <CONFIG extends ModularityOptimizationConfig> NodePropertyValues nodeProperties(
        ComputationResult<ModularityOptimization, ModularityOptimization, CONFIG> computationResult
    ) {
        LongNodePropertyValues resultCommunities = computationResult.result().asNodeProperties();
        if (computationResult.config().consecutiveIds()) {
            return new ConsecutiveLongNodePropertyValues(resultCommunities);
        } else {
            return resultCommunities;
        }
    }

    abstract static class ModularityOptimizationResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {
        long ranIterations;
        boolean didConverge;
        double modularity;

        ModularityOptimizationResultBuilder(
            ProcedureCallContext callContext,
            int concurrency
        ) {
            super(callContext, concurrency);
        }

        ModularityOptimizationResultBuilder<PROC_RESULT> withRanIterations(long ranIterations) {
            this.ranIterations = ranIterations;
            return this;
        }

        ModularityOptimizationResultBuilder<PROC_RESULT> didConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }

        ModularityOptimizationResultBuilder<PROC_RESULT> withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }
    }

}
