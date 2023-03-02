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
package org.neo4j.gds.scaling;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.PropertyMappings.fromObject;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@SuppressWarnings("immutables:subtype")
public interface ScalePropertiesBaseConfig extends AlgoBaseConfig {

    @Configuration.ConvertWith(method = "parsePropertyNames")
    List<String> nodeProperties();

    @Configuration.ConvertWith(method = "org.neo4j.gds.scaling.ScalerFactory#parse")
    @Configuration.ToMapValue("org.neo4j.gds.scaling.ScalerFactory#toString")
    ScalerFactory scaler();

    @Configuration.GraphStoreValidationCheck
    default void validateNodeProperties(
        GraphStore graphStore,
        Collection<NodeLabel> selectedLabels,
        Collection<RelationshipType> selectedRelationshipTypes
    ) {
        if (nodeProperties().size() == 0) {
            throw new IllegalArgumentException("`nodeProperties` must not be empty");
        }

        var missingProperties = nodeProperties()
            .stream()
            .filter(featureProperty -> !graphStore.hasNodeProperty(selectedLabels, featureProperty))
            .collect(Collectors.toList());
        if (!missingProperties.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "The node properties `%s` are not present for all requested labels. " +
                "Requested labels: `%s`. Properties available on all requested labels: `%s`",
                StringJoining.join(missingProperties),
                StringJoining.join(selectedLabels.stream().map(NodeLabel::name)),
                StringJoining.join(graphStore.nodePropertyKeys(selectedLabels))
            ));
        }

        nodeProperties().forEach(propertyName -> {
            if (graphStore.nodeProperty(propertyName).values().dimension().isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node property `%s` contains a `null` value and cannot be scaled. " +
                    "Specifying a default value for the property in the node projection might help.",
                    propertyName
                ));
            }
        });
    }


    @SuppressWarnings("unused")
    static List<String> parsePropertyNames(Object nodePropertiesOrMappings) {
        return fromObject(nodePropertiesOrMappings)
            .mappings()
            .stream()
            .map(PropertyMapping::propertyKey)
            .collect(Collectors.toList());
    }
}
