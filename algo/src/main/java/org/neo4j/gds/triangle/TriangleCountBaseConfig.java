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
package org.neo4j.gds.triangle;

import org.immutables.value.Value;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ValueClass
@Configuration
@SuppressWarnings("immutables:subtype")
public interface TriangleCountBaseConfig extends AlgoBaseConfig {

    @Value.Default
    default long maxDegree() {
        return Long.MAX_VALUE;
    }

    @Value.Check
    default void validateMaxDegree() {
        if (maxDegree() < 2) {
            throw new IllegalArgumentException("The 'maxDegree' parameter must be set to a value greater than 1.");
        }
    }

    @Configuration.GraphStoreValidationCheck
    default void validateTargetRelIsUndirected(
        GraphStore graphStore,
        Collection<NodeLabel> ignored,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (!graphStore.schema().filterRelationshipTypes(Set.copyOf(selectedRelationshipTypes)).isUndirected()) {
            throw new IllegalArgumentException(formatWithLocale(
                "TriangleCount requires relationship projections to be UNDIRECTED. " +
                "Selected relationships `%s` are not all undirected.",
                selectedRelationshipTypes.stream().map(RelationshipType::name).collect(Collectors.toSet())
            ));
        }
    }

    static TriangleCountBaseConfig of(CypherMapWrapper userInput) {
        return new TriangleCountBaseConfigImpl(userInput);
    }
}
