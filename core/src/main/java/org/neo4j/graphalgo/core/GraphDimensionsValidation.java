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

import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.ResolvedPropertyMapping;
import org.neo4j.graphalgo.ResolvedPropertyMappings;
import org.neo4j.graphalgo.api.GraphSetup;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_PROPERTY_KEY;

public final class GraphDimensionsValidation {

    private GraphDimensionsValidation() {}

    public static void validate(GraphDimensions dimensions, GraphSetup setup) {
        checkValidNodePredicate(dimensions, setup);
        checkValidRelationshipTypePredicate(dimensions, setup);
        checkValidProperties("Node", dimensions.nodeProperties());
        checkValidProperties("Relationship", dimensions.relationshipProperties());
    }

    private static void checkValidNodePredicate(GraphDimensions dimensions, GraphSetup setup) {
        if (isNotEmpty(setup.nodeLabel()) && dimensions.nodeLabelIds().contains(NO_SUCH_LABEL)) {
            throw new IllegalArgumentException(String.format(
                "Invalid node projection, one or more labels not found: '%s'",
                setup.nodeLabel()
            ));
        }
    }

    private static void checkValidRelationshipTypePredicate(GraphDimensions dimensions, GraphSetup setup) {
        if (isNotEmpty(setup.relationshipType())) {
            String missingTypes = dimensions.relationshipTypeMappings()
                .stream()
                .filter(m -> !m.doesExist())
                .map(RelationshipTypeMapping::typeName)
                .collect(joining("', '"));
            if (!missingTypes.isEmpty()) {
                throw new IllegalArgumentException(String.format(
                    "Invalid relationship projection, one or more relationship types not found: '%s'",
                    missingTypes));
            }
        }
    }

    private static void checkValidProperties(String recordType, ResolvedPropertyMappings mappings) {
        String missingProperties = mappings
            .stream()
            .filter(mapping -> {
                int id = mapping.propertyKeyId();
                String propertyKey = mapping.neoPropertyKey();
                return isNotEmpty(propertyKey) && id == NO_SUCH_PROPERTY_KEY;
            })
            .map(ResolvedPropertyMapping::neoPropertyKey)
            .collect(joining("', '"));

        if (!missingProperties.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                "%s properties not found: '%s'",
                recordType,
                missingProperties));
        }
    }
}
