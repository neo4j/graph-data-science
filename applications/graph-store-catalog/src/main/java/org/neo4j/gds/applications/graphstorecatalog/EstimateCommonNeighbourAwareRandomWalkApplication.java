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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.samplers.rw.cnarw.CommonNeighbourAwareRandomWalk;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;

class EstimateCommonNeighbourAwareRandomWalkApplication {
    MemoryEstimateResult estimate(
        User user,
        DatabaseId databaseId,
        String graphName,
        CommonNeighbourAwareRandomWalkConfig configuration
    ) {
        var loader = new GraphStoreFromCatalogLoader(
            graphName,
            configuration,
            user.getUsername(),
            databaseId,
            user.isAdmin()
        );

        var memoryTree = CommonNeighbourAwareRandomWalk
            .memoryEstimation(configuration)
            .estimate(loader.graphDimensions(), configuration.concurrency());

        var memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, loader.graphDimensions());

        return new MemoryEstimateResult(memoryTreeWithDimensions);
    }
}
