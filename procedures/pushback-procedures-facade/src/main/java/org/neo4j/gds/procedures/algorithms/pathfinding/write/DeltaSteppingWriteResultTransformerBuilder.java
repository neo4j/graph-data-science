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
package org.neo4j.gds.procedures.algorithms.pathfinding.write;

import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.pathfinding.ShortestPathWriteStep;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;
import org.neo4j.gds.results.ResultTransformerBuilder;

import java.util.stream.Stream;

class DeltaSteppingWriteResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<PathFindingResult>, Stream<StandardWriteRelationshipsResult>> {
    DeltaSteppingWriteResultTransformerBuilder(ShortestPathWriteStep writeStep, AllShortestPathsDeltaWriteConfig config) {}

    @Override
    public ResultTransformer<TimedAlgorithmResult<PathFindingResult>, Stream<StandardWriteRelationshipsResult>> build(
        GraphResources graphResources
    ) {
        return ar -> Stream.empty();
    }
}
