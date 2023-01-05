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
package org.neo4j.gds.beta.indexInverse;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

@Configuration
public interface IndexInverseConfig extends AlgoBaseConfig, MutateRelationshipConfig {
    @Configuration.ConvertWith(method = "validateTypeIdentifier")
    String relationshipType();

    @Override
    @Configuration.Ignore
    default List<String> relationshipTypes() {
        return List.of(relationshipType());
    }

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        return List.of("*");
    }

    static IndexInverseConfig of(CypherMapWrapper configuration) {
        return new IndexInverseConfigImpl(configuration);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateNotIndexed(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (graphStore.inverseIndexedRelationshipTypes().contains(RelationshipType.of(relationshipType()))) {
            throw new IllegalArgumentException(String.format(Locale.US, "Inverse index already exists for '%s'.",
                relationshipType()
            ));
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateNotUndirected(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (graphStore.schema().relationshipSchema().isUndirected(RelationshipType.of(relationshipType()))) {
            throw new UnsupportedOperationException(
                "Creating an inverse index for undirected relationships is not supported.");
        }
    }
}
