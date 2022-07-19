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
package org.neo4j.gds.config;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringIdentifierValidations.emptyToNull;
import static org.neo4j.gds.core.StringIdentifierValidations.validateNoWhiteCharacter;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface SeedConfig {
    String SEED_PROPERTY_KEY = "seedProperty";

    @Value.Default
    @Configuration.ConvertWith("validatePropertyName")
    @Configuration.Key(SEED_PROPERTY_KEY)
    default @Nullable String seedProperty() {
        return null;
    }

    @Configuration.Ignore
    default boolean isIncremental() {
        return seedProperty() != null;
    }

    static @Nullable String validatePropertyName(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), SEED_PROPERTY_KEY);
    }

    @Configuration.GraphStoreValidationCheck
    default void validateSeedProperty(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        String seedProperty = seedProperty();
        if (seedProperty != null && !graphStore.hasNodeProperty(selectedLabels, seedProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Seed property `%s` not found in graph with node properties: %s",
                seedProperty,
                graphStore.nodePropertyKeys().stream().sorted().collect(Collectors.toList())
            ));
        }
    }
}
