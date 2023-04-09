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
package org.neo4j.gds.paths.traverse;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.paths.MutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

public class DfsMutateComputationResultConsumer extends MutateComputationResultConsumer<DFS, HugeLongArray, DfsMutateConfig, MutateResult> {
    DfsMutateComputationResultConsumer() {
        super((computationResult, executionContext) -> new MutateResult.Builder()
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config()));
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<DFS, HugeLongArray, DfsMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        computationResult.result().ifPresent(result -> {
            TraverseMutateResultConsumer.updateGraphStore(
                computationResult.graph(),
                resultBuilder,
                result,
                computationResult.config().mutateRelationshipType(),
                computationResult.graphStore()
            );
        });
    }
}
