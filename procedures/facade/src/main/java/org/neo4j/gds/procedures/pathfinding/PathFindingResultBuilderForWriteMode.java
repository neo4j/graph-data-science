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
package org.neo4j.gds.procedures.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.pathfinding.ResultBuilder;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;

import java.util.Optional;

class PathFindingResultBuilderForWriteMode extends ResultBuilder<PathFindingResult, StandardWriteRelationshipsResult> {
    private final ToMapConvertible configuration;

    PathFindingResultBuilderForWriteMode(ToMapConvertible configuration) {
        this.configuration = configuration;
    }

    @Override
    public StandardWriteRelationshipsResult build(
        Graph graph,
        GraphStore graphStore,
        Optional<PathFindingResult> pathFindingResult
    ) {
        return new StandardWriteRelationshipsResult(
            preProcessingMillis,
            computeMillis,
            0, // yeah, I don't understand it either :shrug:
            postProcessingMillis,
            relationshipsWritten,
            configuration.toMap()
        );
    }
}
