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

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodePropertyMustExistOnAnyLabel implements AlgorithmGraphStoreRequirements {

    private final String nodeProperty;

    public NodePropertyMustExistOnAnyLabel(String nodeProperty) {this.nodeProperty = nodeProperty;}

    @Override
    public void validate(
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
        if (selectedLabels
            .stream()
            .anyMatch(label -> graphStore.nodePropertyKeys(label).contains(nodeProperty))) {
            return;
        }
        throw new IllegalArgumentException(formatWithLocale(
            "Node Property `%s` is not present for any requested node labels. Requested labels: %s. Labels with `%1$s` present: %s",
            nodeProperty,
            StringJoining.join(selectedLabels.stream().map(NodeLabel::name)),
            StringJoining.join(graphStore
                .nodeLabels()
                .stream()
                .filter(label -> graphStore.nodePropertyKeys(label).contains(nodeProperty))
                .map(NodeLabel::name))
        ));
    }
}
