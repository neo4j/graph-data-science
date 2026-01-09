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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.paths.TraversalRelationshipTransformer;

public class SearchMutateStep implements MutateStep<HugeLongArray, RelationshipsWritten> {
    private final String  mutateRelationshipType;
    private final MutateRelationshipService mutateRelationshipService;

    public SearchMutateStep(MutateRelationshipService mutateRelationshipService, String mutateRelationshipType) {
        this.mutateRelationshipType = mutateRelationshipType;
        this.mutateRelationshipService = mutateRelationshipService;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        HugeLongArray result
    ) {
        if (result.size() == 0) return  new RelationshipsWritten(0);

        var relationships = TraversalRelationshipTransformer.buildRelationships(graph, mutateRelationshipType, result);

        return  mutateRelationshipService.mutate(graphStore,relationships);
    }
}
