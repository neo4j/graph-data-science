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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsAndDirection;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Optional;

final class TraverseMutateResultConsumer {

    private TraverseMutateResultConsumer() {}

    static void updateGraphStore(
        Graph graph,
        AbstractResultBuilder<?> resultBuilder,
        HugeLongArray result,
        String relationshipType,
        GraphStore graphStore
    ) {
        var mutateRelationshipType = RelationshipType.of(relationshipType);

        var relationshipsBuilder = GraphFactory
            .initRelationshipsBuilder()
            .nodes(graph)
            .direction(Direction.DIRECTED)
            .build();

        RelationshipsAndDirection RelationshipsAndDirection;

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            var source = result.get(0);
            for (long i = 1; i < result.size(); i++) {
                var target = result.get(i);
                relationshipsBuilder.addFromInternal(source, target);
                source = target;
            }

            RelationshipsAndDirection = relationshipsBuilder.build();
            resultBuilder.withRelationshipsWritten(RelationshipsAndDirection.relationships().topology().elementCount());
        }

        graphStore
            .addRelationshipType(
                mutateRelationshipType,
                Optional.empty(),
                Optional.empty(),
                RelationshipsAndDirection
            );
    }
}
