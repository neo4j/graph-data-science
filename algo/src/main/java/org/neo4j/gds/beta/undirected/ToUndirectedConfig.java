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
package org.neo4j.gds.beta.undirected;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;

@Configuration
public interface ToUndirectedConfig extends AlgoBaseConfig, MutateRelationshipConfig {
    @Configuration.ConvertWith(method = "validateRelationshipTypeIdentifier")
    String relationshipType();

    @Override
    @Configuration.Ignore
    default List<String> relationshipTypes() {
        return List.of("*");
    }

    @Override
    @Configuration.Ignore
    default List<String> nodeLabels() {
        return List.of("*");
    }

    static ToUndirectedConfig of(CypherMapWrapper configuration) {
        return new ToUndirectedConfigImpl(configuration);
    }

    @Value.Check
    default void validateRelationshipTypeNotStar() {
        if (relationshipType().equals(ElementProjection.PROJECT_ALL)) {
            throw new UnsupportedOperationException("`relationshipType` cannot be `*`. Please specify the concrete relationship type.");
        }
    }

    static @Nullable String validateRelationshipTypeIdentifier(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), "relationshipType");
    }
}
