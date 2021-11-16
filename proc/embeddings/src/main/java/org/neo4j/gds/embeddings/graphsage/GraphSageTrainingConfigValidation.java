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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.validation.AfterLoadValidation;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GraphSageTrainingConfigValidation implements AfterLoadValidation<GraphSageTrainConfig> {
    @Override
    public void validateConfigsAfterLoad(
        GraphStore graphStore, GraphCreateConfig graphCreateConfig, GraphSageTrainConfig config
    ) {
        var nodeLabels = config.nodeLabelIdentifiers(graphStore);
        var nodePropertyNames = config.featureProperties();

        if (config.isMultiLabel()) {
            // each property exists on at least one label
            var allProperties =
                graphStore
                    .schema()
                    .nodeSchema()
                    .allProperties();
            var missingProperties = nodePropertyNames
                .stream()
                .filter(key -> !allProperties.contains(key))
                .collect(Collectors.toSet());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Each property set in `featureProperties` must exist for at least one label. Missing properties: %s",
                    missingProperties
                ));
            }
        } else {
            // all properties exist on all labels
            List<String> missingProperties = nodePropertyNames
                .stream()
                .filter(weightProperty -> !graphStore.hasNodeProperty(nodeLabels, weightProperty))
                .collect(Collectors.toList());
            if (!missingProperties.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The following node properties are not present for each label in the graph: %s. Properties that exist for each label are %s",
                    missingProperties,
                    graphStore.nodePropertyKeys(nodeLabels)
                ));
            }
        }
    }
}
