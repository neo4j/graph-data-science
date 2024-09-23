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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.utils.progress.JobId;

import java.util.Optional;
import java.util.function.Function;

class WriteSideEffect<RESULT_FROM_ALGORITHM, WRITE_METADATA> implements SideEffect<RESULT_FROM_ALGORITHM, WRITE_METADATA> {
    private final SideEffectExecutor sideEffectExecutor = new SideEffectExecutor();

    private final JobId jobId;
    private final WriteStep<RESULT_FROM_ALGORITHM, WRITE_METADATA> writeStep;

    WriteSideEffect(JobId jobId, WriteStep<RESULT_FROM_ALGORITHM, WRITE_METADATA> writeStep) {
        this.jobId = jobId;
        this.writeStep = writeStep;
    }

    @Override
    public Optional<WRITE_METADATA> process(
        GraphResources graphResources,
        Optional<RESULT_FROM_ALGORITHM> result
    ) {
        Function<RESULT_FROM_ALGORITHM, WRITE_METADATA> writeEffect = r -> writeStep.execute(
            graphResources.graph(),
            graphResources.graphStore(),
            graphResources.resultStore(),
            r,
            jobId
        );

        return sideEffectExecutor.executeSideEffect(result, writeEffect);
    }
}
