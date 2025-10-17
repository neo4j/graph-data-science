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
package org.neo4j.gds.core.loading.validation;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodePropertyAllExistsGraphStoreValidation extends GraphStoreValidation {

    private final String nodeProperty;

    public NodePropertyAllExistsGraphStoreValidation(String nodeProperty) {this.nodeProperty = nodeProperty;}

    @Override
    public void validateAlgorithmRequirements(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        validatePropertyExists(graphStore, selectedLabels);
    }

    void validatePropertyExists(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels
    ) {
        if (nodeProperty!=null){
        if (!graphStore.hasNodeProperty(selectedLabels, nodeProperty)) {
            var labelsWithMissingProperty = selectedLabels
                .stream()
                .filter(label -> !graphStore.nodePropertyKeys(label).contains(nodeProperty))
                .map(NodeLabel::name)
                .collect(Collectors.toList());

            throw new IllegalArgumentException(formatWithLocale(
                "Node property `%s` is not present for all requested labels. Requested labels: %s. Labels without the property key: %s. Properties available on all requested labels: %s",
                nodeProperty,
                StringJoining.join(selectedLabels.stream().map(NodeLabel::name)),
                StringJoining.join(labelsWithMissingProperty),
                StringJoining.join(graphStore.nodePropertyKeys(selectedLabels))
            ));
        }
        }

    }
}
