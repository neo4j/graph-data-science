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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.StringSimilarity;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GraphStoreValidationService {
    /**
     * @throws java.lang.IllegalArgumentException if at least one key in the list of node properties is not present in the graph store
     */
    public void ensureNodePropertiesExist(GraphStore graphStore, Collection<String> nodeProperties) {
        var invalidProperties = nodeProperties.stream()
            .filter(nodeProperty -> !graphStore.hasNodeProperty(nodeProperty))
            .collect(Collectors.toList());

        if (!invalidProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not find property key(s) %s. Defined keys: %s.",
                StringJoining.join(invalidProperties),
                StringJoining.join(graphStore.nodePropertyKeys())
            ));
        }
    }

    public void ensureRelationshipsMayBeDeleted(GraphStore graphStore, String relationshipType, GraphName graphName) {
        var relationshipTypes = graphStore.relationshipTypes();

        if (relationshipTypes.size() == 1) {
            throw new IllegalArgumentException(formatWithLocale(
                "Deleting the last relationship type ('%s') from a graph ('%s') is not supported. " +
                    "Use `gds.graph.drop()` to drop the entire graph instead.",
                relationshipType,
                graphName
            ));
        }

        if (!relationshipTypes.contains(RelationshipType.of(relationshipType))) {
            throw new IllegalArgumentException(formatWithLocale(
                "No relationship type '%s' found in graph '%s'.",
                relationshipType,
                graphName
            ));
        }
    }

    public void ensureGraphPropertyExists(GraphStore graphStore, String graphProperty) {
        if (graphStore.hasGraphProperty(graphProperty)) return;

        var candidates = StringSimilarity.similarStringsIgnoreCase(
            graphProperty,
            graphStore.graphPropertyKeys()
        );

        if (candidates.isEmpty()) {
            var detailMessage = formatWithLocale(
                "The following properties exist in the graph %s.",
                StringJoining.join(graphStore.graphPropertyKeys())
            );
            errorOnMissingGraphProperty(graphProperty, detailMessage);
        }

        var detailMessage = formatWithLocale("Did you mean: %s.", StringJoining.join(candidates));
        errorOnMissingGraphProperty(graphProperty, detailMessage);
    }

    private void errorOnMissingGraphProperty(String graphProperty, String detailMessage) {
        throw new IllegalArgumentException(formatWithLocale(
            "The specified graph property '%s' does not exist. %s",
            graphProperty,
            detailMessage
        ));
    }
}
