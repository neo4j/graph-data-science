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
package org.neo4j.gds.embeddings.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class FeaturePropertiesMustExistOnAllNodeLabels extends GraphStoreValidation {

    private final List<String> featureProperties;

    public FeaturePropertiesMustExistOnAllNodeLabels(List<String> featureProperties) {
        this.featureProperties = featureProperties;
    }

    @Override
    protected void validateAlgorithmRequirements(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {

        var missingProperties = featureProperties
            .stream()
            .filter(featureProperty -> !graphStore.hasNodeProperty(selectedLabels, featureProperty))
            .collect(Collectors.toList());

        if (!missingProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The feature properties %s are not present for all requested labels. " +
                    "Requested labels: %s. Properties available on all requested labels: %s",
                StringJoining.join(missingProperties),
                StringJoining.join(selectedLabels.stream().map(NodeLabel::name)),
                StringJoining.join(graphStore.nodePropertyKeys(selectedLabels))
            ));
        }
    }
}
