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

import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.procedures.algorithms.community.ModularityOptimizationResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;

final class ModularityOptimizationSpecificationHelper {
    static final String MODULARITY_OPTIMIZATION_DESCRIPTION = "The Modularity Optimization algorithm groups the nodes in the graph by optimizing the graphs modularity.";

    static <PROC_RESULT, CONFIG extends ModularityOptimizationBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        ModularityOptimizationResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<ModularityOptimization, ModularityOptimizationResult, CONFIG> computeResult
    ) {
        computeResult.result().ifPresent(result -> {
            procResultBuilder
                .withModularity(result.modularity())
                .withRanIterations(result.ranIterations())
                .didConverge(result.didConverge())
                .withCommunityFunction(result::communityId);
        });
        return procResultBuilder
            .withConfig(computeResult.config());

    }

    private ModularityOptimizationSpecificationHelper() {}
}
