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
import org.neo4j.gds.StreamOfRelationshipsWriter;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.stream.Stream;

public abstract class ShortestPathWriteProc<ALGO extends Algorithm<DijkstraResult>, CONFIG extends AlgoBaseConfig & WriteRelationshipConfig & WritePathOptionsConfig>
    extends StreamOfRelationshipsWriter<ALGO, DijkstraResult, CONFIG, StandardWriteRelationshipsResult> {

    protected Stream<StandardWriteRelationshipsResult> write(ComputationResult<ALGO, DijkstraResult, CONFIG> computationResult) {
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ComputationResultConsumer<ALGO, DijkstraResult, CONFIG, Stream<StandardWriteRelationshipsResult>> computationResultConsumer() {
        return new ShortestPathWriteResultConsumer<>();
    }

@Override
    public boolean releaseProgressTask() {
        return false;
    }
}
