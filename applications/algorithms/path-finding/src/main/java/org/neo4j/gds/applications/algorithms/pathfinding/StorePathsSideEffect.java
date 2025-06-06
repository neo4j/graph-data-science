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

import org.neo4j.gds.api.ExportedRelationship;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class StorePathsSideEffect implements SideEffect<PathFindingResult, Void> {
    private final ResultStore resultStore;
    private final String relationshipTypeAsString;
    private final List<String> propertyKeys;
    private final List<ValueType> propertyTypes;

    public StorePathsSideEffect(
        ResultStore resultStore,
        String relationshipTypeAsString,
        List<String> propertyKeys,
        List<ValueType> propertyTypes
    ) {
        this.resultStore = resultStore;
        this.relationshipTypeAsString = relationshipTypeAsString;
        this.propertyKeys = propertyKeys;
        this.propertyTypes = propertyTypes;
    }

    @Override
    public Optional<Void> process(GraphResources graphResources, Optional<PathFindingResult> pathFindingResult) {
        if (pathFindingResult.isEmpty()) {
            return Optional.empty();
        }
        var actualPathResult  = pathFindingResult.get();
        Stream<ExportedRelationship> relationshipStream = actualPathResult.mapPaths(
            pathResult -> new ExportedRelationship(
                pathResult.sourceNode(),
                pathResult.targetNode(),
                createValues(pathResult)
            ));

        ResultStoreEntry.RelationshipStream streamEntry = new ResultStoreEntry.RelationshipStream(
            relationshipTypeAsString,
            propertyKeys,
            propertyTypes,
            relationshipStream,
            graphResources.graph()::toOriginalNodeId
        );
        resultStore.add(JobId.parse(this.relationshipTypeAsString), streamEntry);

        return Optional.empty();
    }

    private Value[] createValues(PathResult pathResult) {
        return new Value[]{
            Values.doubleValue(pathResult.totalCost()),
            Values.longArray(pathResult.nodeIds()),
            Values.doubleArray(pathResult.costs())
        };
    }
}
