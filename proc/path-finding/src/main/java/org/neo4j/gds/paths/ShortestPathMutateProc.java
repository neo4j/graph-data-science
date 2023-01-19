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
package org.neo4j.gds.paths;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.MutateProc;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.result.AbstractResultBuilder;

public abstract class ShortestPathMutateProc<ALGO extends Algorithm<DijkstraResult>, CONFIG extends AlgoBaseConfig & MutateRelationshipConfig> extends MutateProc<ALGO, DijkstraResult, MutateResult, CONFIG> {

    @SuppressWarnings("unchecked")
    @Override
    public MutateComputationResultConsumer<ALGO, DijkstraResult, CONFIG, MutateResult> computationResultConsumer() {
        return new ShortestPathMutateResultConsumer<>();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ALGO, DijkstraResult, CONFIG> computeResult, ExecutionContext executionContext
    ) {
        return new MutateResult.Builder()
            .withPreProcessingMillis(computeResult.preProcessingMillis())
            .withComputeMillis(computeResult.computeMillis())
            .withConfig(computeResult.config());
    }

@Override
    public boolean releaseProgressTask() {
        return false;
    }

}
