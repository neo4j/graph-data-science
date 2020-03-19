/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.beta.modularity;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

final class ModularityOptimizationProc {
    static final String MODULARITY_OPTIMIZATION_DESCRIPTION = "The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.";

    static <PROC_RESULT, CONFIG extends ModularityOptimizationConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        ModularityOptimizationResultBuilder<PROC_RESULT> procResultBuilder,
        AlgoBaseProc.ComputationResult<ModularityOptimization, ModularityOptimization, CONFIG> computeResult
    ) {
        ModularityOptimization result = computeResult.result();

        return procResultBuilder
            .withModularity(result.getModularity())
            .withRanIterations(result.getIterations())
            .didConverge(result.didConverge())
            .withCommunityFunction(result::getCommunityId)
            .withNodeCount(computeResult.graph().nodeCount())
            .withConfig(computeResult.config());

    }

    static final class ModularityOptimizationTranslator implements PropertyTranslator.OfLong<ModularityOptimization> {
        public static final ModularityOptimizationTranslator INSTANCE = new ModularityOptimizationTranslator();

        @Override
        public long toLong(ModularityOptimization data, long nodeId) {
            return data.getCommunityId(nodeId);
        }
    }

    abstract static class ModularityOptimizationResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {
        long ranIterations;
        boolean didConverge;
        double modularity;

        ModularityOptimizationResultBuilder(
            ProcedureCallContext callContext,
            AllocationTracker tracker
        ) {
            super(callContext, tracker);
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
