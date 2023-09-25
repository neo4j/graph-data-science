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
package org.neo4j.gds.projection;


import org.neo4j.gds.core.GraphDimensions;

import java.util.Map;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_LABEL;
import static org.neo4j.gds.core.GraphDimensions.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

final class GraphDimensionsValidation {

    private GraphDimensionsValidation() {}

    public static void validate(GraphDimensions dimensions, GraphProjectFromStoreConfig config) {
        checkValidNodePredicate(dimensions, config);
        checkValidPropertyTokens("Node", dimensions.nodePropertyTokens());
        checkValidRelationshipTypePredicate(dimensions, config);
        checkValidPropertyTokens("Relationship", dimensions.relationshipPropertyTokens());
    }

    private static void checkValidNodePredicate(GraphDimensions dimensions, GraphProjectFromStoreConfig config) {
        if (!config.nodeProjections().isEmpty() && dimensions.nodeLabelTokens().contains(NO_SUCH_LABEL)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid node projection, one or more labels not found: '%s'",
                config.nodeProjections().labelProjection()
            ));
        }
    }

    private static void checkValidRelationshipTypePredicate(
        GraphDimensions dimensions,
        GraphProjectFromStoreConfig config
    ) {
        if (!config.relationshipProjections().isEmpty() && dimensions
            .relationshipTypeTokens()
            .contains(NO_SUCH_RELATIONSHIP_TYPE)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid relationship projection, one or more relationship types not found: '%s'",
                config.relationshipProjections().typeFilter()
            ));
        }
    }

    private static void checkValidPropertyTokens(String recordType, Map<String, Integer> propertyIds) {
        String missingProperties = propertyIds
            .entrySet()
            .stream()
            .filter(mapping -> {
                String propertyKey = mapping.getKey();
                int id = mapping.getValue();
                return (isNotEmpty(propertyKey) && id == NO_SUCH_PROPERTY_KEY) && !propertyKey.equals("*");
            })
            .map(Map.Entry::getKey)
            .collect(joining("', '"));

        if (!missingProperties.isEmpty()) {
            String errorMessage = formatWithLocale(
                "%s properties not found: '%s'",
                recordType,
                missingProperties
            );

            errorMessage += recordType.equals("Relationship") ? " (if you meant to count parallel relationships, use `property:'*'`)." : "";
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
