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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.LongObjectMap;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.core.loading.CypherNodeLoader.CYPHER_RESULT_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.ANY_LABEL;
import static org.neo4j.graphalgo.core.loading.NodesBatchBuffer.IGNORE_LABEL;

public class CypherNodePropertyImporter {

    public static final double NO_PROPERTY_VALUE = Double.NaN;

    private final Collection<String> propertyColumns;
    private final long nodeCount;
    private final int concurrency;
    private final LongObjectMap<List<ElementIdentifier>> labelElementIdentifierMapping;
    private final Map<ElementIdentifier, Map<String, NodePropertiesBuilder>> buildersByIdentifier;


    public CypherNodePropertyImporter(
        Collection<String> propertyColumns,
        LongObjectMap<List<ElementIdentifier>> labelElementIdentifierMapping,
        long nodeCount,
        int concurrency
    ) {
        this.propertyColumns = propertyColumns;
        this.labelElementIdentifierMapping = labelElementIdentifierMapping;
        this.nodeCount = nodeCount;
        this.concurrency = concurrency;

        this.buildersByIdentifier = new HashMap<>();
    }

    public Collection<String> propertyColumns() {
        return propertyColumns;
    }

    public void registerPropertiesForLabels(List<String> labels) {
        for (String label : labels) {
            ElementIdentifier labelIdentifier = new ElementIdentifier(label);
            Map<String, NodePropertiesBuilder> propertyBuilders = buildersByIdentifier.computeIfAbsent(labelIdentifier, (ignore) -> new HashMap<>());
            for (String property : propertyColumns) {
                propertyBuilders.computeIfAbsent(
                        property,
                        (ignore) -> NodePropertiesBuilder.of(
                                nodeCount,
                                AllocationTracker.EMPTY,
                            NO_PROPERTY_VALUE,
                                CYPHER_RESULT_PROPERTY_KEY,
                                property,
                                concurrency
                        )
                );
            }
        }
    }

    public int importProperties(long nodeId, long[] labels, Map<String, Number> nodeProperties) {
        int propertiesImported = 0;

        // If there is a node projection for ANY label, then we need to consume the node properties regardless.
        propertiesImported += setPropertyForLabel(PROJECT_ALL, nodeProperties, nodeId);

        for (long label : labels) {
            if (label == IGNORE_LABEL || label == ANY_LABEL) {
                continue;
            }

            for(ElementIdentifier labelIdentifier : labelElementIdentifierMapping.get(label)) {
                propertiesImported += setPropertyForLabel(labelIdentifier, nodeProperties, nodeId);
            }
        }

        return propertiesImported;
    }

    public Map<ElementIdentifier, Map<PropertyMapping, NodeProperties>> result() {
        return buildersByIdentifier
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().entrySet().stream().collect(Collectors.toMap(
                                builderEntry -> PropertyMapping.of(builderEntry.getKey(), Double.NaN),
                                builderEntry -> builderEntry.getValue().build()
                        ))
                ));
    }

    private int setPropertyForLabel(ElementIdentifier labelIdentifier, Map<String, Number> nodeProperties, long nodeId) {
        int propertiesImported = 0;

        if (buildersByIdentifier.containsKey(labelIdentifier)) {
            Map<String, NodePropertiesBuilder> buildersByProperty = buildersByIdentifier.get(labelIdentifier);

            for (Map.Entry<String, Number> propertyEntry : nodeProperties.entrySet()) {
                if (buildersByProperty.containsKey(propertyEntry.getKey())) {
                    NodePropertiesBuilder builder = buildersByProperty.get(propertyEntry.getKey());
                    builder.set(nodeId, propertyEntry.getValue().doubleValue());
                    propertiesImported++;
                }
            }
        }

        return propertiesImported;
    }

}
