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

public interface ConfigurableSeedConfig {

    @Value.Default
    @Configuration.ConvertWith("validateProperty")
    default @Nullable String seedProperty() {
        return null;
    }

    @Configuration.Ignore
    default String propertyNameOverride() {
        return "seedProperty";
    }

    static @Nullable String validateProperty(String input) {
        return validateNoWhiteCharacter(emptyToNull(input), "seedProperty");
    }

    @Configuration.GraphStoreValidationCheck
    default void validateConfigurableSeedConfig(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        String seedProperty = seedProperty();
        if (seedProperty != null && !graphStore.hasNodeProperty(selectedLabels, seedProperty)) {
            throw new IllegalArgumentException(formatWithLocale(
                "`%s`: `%s` not found in graph with node properties: %s",
                propertyNameOverride(),
                seedProperty,
                graphStore.nodePropertyKeys().stream().sorted().collect(Collectors.toList())
            ));
        }
    }
}
