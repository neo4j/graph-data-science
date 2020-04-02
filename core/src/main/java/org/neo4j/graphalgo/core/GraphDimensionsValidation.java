/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.RelationshipProjectionMapping;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.GraphSetup;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.compat.StatementConstantsProxy.NO_SUCH_LABEL;
import static org.neo4j.graphalgo.compat.StatementConstantsProxy.NO_SUCH_PROPERTY_KEY;

public final class GraphDimensionsValidation {

    private GraphDimensionsValidation() {}

    public static void validate(GraphDimensions dimensions, GraphSetup setup) {
        checkValidNodePredicate(dimensions, setup);
        checkValidRelationshipTypePredicate(dimensions, setup);
        checkValidProperties("Node", dimensions.nodeProperties());
        checkValidProperties("Relationship", dimensions.relationshipProperties());
    }

    private static void checkValidNodePredicate(GraphDimensions dimensions, GraphSetup setup) {
        if (!setup.nodeProjections().isEmpty() && dimensions.nodeLabelIds().contains(NO_SUCH_LABEL)) {
            throw new IllegalArgumentException(String.format(
                "Invalid node projection, one or more labels not found: '%s'",
                setup.nodeProjections().labelProjection()
            ));
        }
    }

    private static void checkValidRelationshipTypePredicate(GraphDimensions dimensions, GraphSetup setup) {
        if (isNotEmpty(setup.relationshipType())) {
            String missingTypes = dimensions.relationshipProjectionMappings()
                .stream()
                .filter(m -> !m.exists() && !m.typeName().equals(PROJECT_ALL.name))
                .map(RelationshipProjectionMapping::typeName)
                .collect(joining("', '"));
            if (!missingTypes.isEmpty()) {
                throw new IllegalArgumentException(String.format(
                    "Invalid relationship projection, one or more relationship types not found: '%s'",
                    missingTypes
                ));
            }
        }
    }

    private static void checkValidProperties(String recordType, ResolvedPropertyMappings mappings) {
        List<ResolvedPropertyMapping> invalidProperties = mappings
            .stream()
            .filter(mapping -> {
                int id = mapping.propertyKeyId();
                if (id != NO_SUCH_PROPERTY_KEY) {
                    return false;
                }
                String propertyKey = mapping.neoPropertyKey();
                if (mapping.aggregation() == Aggregation.COUNT && "*".equals(propertyKey)) {
                    return false;
                }
                return true;
            })
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            String missingPropertiesMessage = invalidProperties
                .stream()
                .map(mapping -> {
                    String propertyKey = mapping.neoPropertyKey();
                    if (mapping.aggregation() == Aggregation.COUNT &&
                        propertyKey.equals(mapping.propertyKey())) {
                        return String.format(
                            "'%s' (if you meant to count parallel relationships, use `property:'*'`)",
                            propertyKey
                        );
                    }
                    return String.format("'%s'", propertyKey);
                })
                .collect(joining(", "));

            throw new IllegalArgumentException(String.format(
                "%s properties not found: %s.",
                recordType,
                missingPropertiesMessage
            ));
        }
    }
}
