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

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.catalog.UserInputAsStringOrListOfString;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface GraphExportNodePropertiesConfig extends GraphNodePropertiesConfig {

    @Configuration.Parameter
    @Configuration.ConvertWith(method = "org.neo4j.gds.config.GraphExportNodePropertiesConfig#parseNodeProperties")
    List<String> nodeProperties();

    static List<String> parseNodeProperties(Object userInput) {
        
        return UserInputAsStringOrListOfString.parse(userInput, "nodeProperties");
    }

    @Configuration.Ignore
    default void validate(GraphStore graphStore) {
        if (!nodeLabels().contains(ElementProjection.PROJECT_ALL)) {
            // validate that all given labels have all the properties
            nodeLabelIdentifiers(graphStore).forEach(nodeLabel -> {
                    List<String> invalidProperties = nodeProperties()
                        .stream()
                        .filter(nodeProperty -> !graphStore.hasNodeProperty(List.of(nodeLabel), nodeProperty))
                        .collect(Collectors.toList());

                    if (!invalidProperties.isEmpty()) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Expecting all specified node projections to have all given properties defined. " +
                            "Could not find property key(s) %s for label %s. Defined keys: %s.",
                            StringJoining.join(invalidProperties),
                            nodeLabel.name,
                            StringJoining.join(graphStore.nodePropertyKeys(nodeLabel))
                        ));
                    }
                }
            );
        } else {
            // validate that at least one label has all the properties
            boolean hasValidLabel = nodeLabelIdentifiers(graphStore).stream()
                .anyMatch(nodeLabel -> graphStore
                    .nodePropertyKeys(List.of(nodeLabel))
                    .containsAll(nodeProperties()));

            if (!hasValidLabel) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Expecting at least one node projection to contain property key(s) %s.",
                    StringJoining.join(nodeProperties())
                ));
            }
        }
    }

    /**
     * Returns the node labels that are to be considered for writing properties.
     * <p>
     * If nodeLabels contains '*`, this returns all node labels in the graph store
     * that have the specified nodeProperties.
     * <p>
     * Otherwise, it just returns all the labels in the graph store since validation
     * made sure that all node labels have the specified properties.
     */
    @Configuration.Ignore
    default Collection<NodeLabel> validNodeLabels(GraphStore graphStore) {
        Collection<NodeLabel> filteredNodeLabels;

        if (nodeLabels().contains(ElementProjection.PROJECT_ALL)) {
            // Filter node labels that have all the properties.
            // Validation guarantees that there is at least one.
            filteredNodeLabels = nodeLabelIdentifiers(graphStore)
                .stream()
                .filter(nodeLabel -> graphStore.nodePropertyKeys(nodeLabel).containsAll(nodeProperties()))
                .collect(Collectors.toList());
        } else {
            // Write for all the labels that are specified.
            // Validation guarantees that each label has all properties.
            filteredNodeLabels = nodeLabelIdentifiers(graphStore);
        }

        return filteredNodeLabels;
    }
}
