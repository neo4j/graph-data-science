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

import java.util.Collection;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class NodePropertyMustNotExistOnAnyLabels implements AlgorithmGraphStoreRequirements {
    private final String nodeProperty;

    public NodePropertyMustNotExistOnAnyLabels(String nodeProperty) {
        this.nodeProperty = nodeProperty;
    }

    @Override
    public void validate(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (nodeProperty != null) {
            for (var label : selectedLabels) {
                //restricted from org.neo4j.gds.config.MutateNodePropertyConfig.validateMutateProperty
                //which is more lax i.e., does not fail so long as one node does not have the property\
                //(it fails at a later step anyway)
                if (graphStore.hasNodeProperty(label, nodeProperty)) {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Node property `%s` already exists in the in-memory graph.",
                        nodeProperty
                    ));
                }
            }
        }
    }
}
