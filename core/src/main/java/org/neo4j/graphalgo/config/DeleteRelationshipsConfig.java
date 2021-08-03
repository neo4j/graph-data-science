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
package org.neo4j.graphalgo.config;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;

import java.util.Set;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface DeleteRelationshipsConfig {

    @Configuration.Parameter
    String graphName();

    @Configuration.Parameter
    String relationshipType();

    static DeleteRelationshipsConfig of(
        String graphName,
        String relationshipType
    ) {
        return new DeleteRelationshipsConfigImpl(
            graphName,
            relationshipType
        );
    }

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {

        Set<RelationshipType> relationshipTypes = graphStore.relationshipTypes();

        if (relationshipTypes.size() == 1) {
            throw new IllegalArgumentException(formatWithLocale(
                "Deleting the last relationship type ('%s') from a graph ('%s') is not supported. " +
                "Use `gds.graph.drop()` to drop the entire graph instead.",
                relationshipType(),
                graphName()
            ));
        }

        if (!relationshipTypes.contains(RelationshipType.of(relationshipType()))) {
            throw new IllegalArgumentException(formatWithLocale(
                "No relationship type '%s' found in graph '%s'.",
                relationshipType(),
                graphName()
            ));
        }
    }
}
