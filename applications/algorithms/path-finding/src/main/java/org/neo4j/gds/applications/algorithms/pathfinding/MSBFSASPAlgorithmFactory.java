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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.allshortestpaths.MSBFSASPAlgorithm;
import org.neo4j.gds.allshortestpaths.MSBFSAllShortestPaths;
import org.neo4j.gds.allshortestpaths.WeightedAllShortestPaths;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;

public final class MSBFSASPAlgorithmFactory {
    private MSBFSASPAlgorithmFactory() {}

    public static MSBFSASPAlgorithm create(
        Graph graph,
        AllShortestPathsParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        if (parameters.hasRelationshipWeightProperty()) {
            // TODO: do we need this check,
            //  this should be handled by the relationship property validation
            //  see `org.neo4j.gds.core.loading.GraphStoreValidation.validateRelationshipProperty`
            if (!graph.hasRelationshipProperty()) {
                throw new IllegalArgumentException(
                    "WeightedAllShortestPaths is not supported on graphs without a weight property");
            }

            return new WeightedAllShortestPaths(
                graph,
                executorService,
                parameters.concurrency(),
                progressTracker,
                terminationFlag
            );
        } else {
            return new MSBFSAllShortestPaths(
                graph,
                parameters.concurrency(),
                executorService,
                progressTracker,
                terminationFlag
            );
        }
    }
}
