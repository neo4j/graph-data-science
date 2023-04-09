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

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;

import java.util.stream.Stream;

class BfsStreamComputationResultConsumer implements ComputationResultConsumer<BFS, HugeLongArray, BfsStreamConfig, Stream<BfsStreamResult>> {

    private final PathFactoryFacade pathFactoryFacade;

    BfsStreamComputationResultConsumer(PathFactoryFacade pathFactoryFacade) {
        this.pathFactoryFacade = pathFactoryFacade;
    }

    @Override
    public Stream<BfsStreamResult> consume(
        ComputationResult<BFS, HugeLongArray, BfsStreamConfig> computationResult,
        ExecutionContext executionContext
    ) {
        if (computationResult.result().isEmpty()) {
            return Stream.empty();
        }

        return TraverseStreamComputationResultConsumer.consume(
            computationResult.config().sourceNode(),
            computationResult.result().get(),
            computationResult.graph()::toOriginalNodeId,
            computationResult.graph().isEmpty(),
            BfsStreamResult::new,
            executionContext.returnColumns().contains("path"),
            pathFactoryFacade,
            BfsStreamProc.NEXT,
            executionContext.nodeLookup()
        );
    }
}
