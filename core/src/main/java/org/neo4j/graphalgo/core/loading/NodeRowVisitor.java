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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphdb.Result;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private static final String ID_COLUMN = "id";
    static final String LABELS_COLUMN = "labels";
    static final Set<String> RESERVED_COLUMNS = Set.of(ID_COLUMN, LABELS_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = Set.of(ID_COLUMN);

    private long rows;
    private long maxNeoId = 0;
    private final NodesBatchBuffer buffer;
    private final List<Map<String, Value>> cypherNodeProperties;
    private final NodeImporter importer;
    private final boolean hasLabelInformation;
    private final CypherNodePropertyImporter propertyImporter;

    private final Map<ElementIdentifier, Integer> elementIdentifierLabelTokenMapping;
    private int labelIdCounter = 0;

    public NodeRowVisitor(
        NodesBatchBuffer buffer,
        NodeImporter importer,
        boolean hasLabelInformation,
        CypherNodePropertyImporter propertyImporter
    ) {
        this.buffer = buffer;
        this.importer = importer;
        this.cypherNodeProperties = new ArrayList<>(buffer.capacity());
        this.hasLabelInformation = hasLabelInformation;
        this.propertyImporter = propertyImporter;
        this.elementIdentifierLabelTokenMapping = new HashMap<>();

    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        long neoId = row.getNumber(ID_COLUMN).longValue();
        if (neoId > maxNeoId) {
            maxNeoId = neoId;
        }
        rows++;

        List<String> labels = getLabels(row, neoId);
        long[] labelIds = computeLabelIds(labels);

        int propRef = processProperties(row, labels);

        buffer.add(neoId, propRef, labelIds);
        if (buffer.isFull()) {
            flush();
            reset();
        }
        return true;
    }

    void flush() {
        if (rows == 0) {
            throw new IllegalArgumentException("Node-Query returned no nodes");
        }
        importer.importCypherNodes(buffer, cypherNodeProperties, propertyImporter);
    }

    private List<String> getLabels(Result.ResultRow row, long neoId) {
        if (hasLabelInformation) {
            Object labelsObject = row.get(LABELS_COLUMN);
            if (!(labelsObject instanceof List)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Type of column `%s` should be of type List, but was `%s`",
                    LABELS_COLUMN,
                    labelsObject
                ));
            }

            List<String> labels = (List<String>) labelsObject;

            if (labels.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node(%d) does not specify a label, but label column '%s' was specified.",
                    neoId,
                    LABELS_COLUMN
                ));
            }

            return labels;
        } else {
            return Collections.singletonList(ALL_NODES.name);
        }
    }

    private long[] computeLabelIds(List<String> labels) {
        long[] labelIds = new long[labels.size()];

        for (int i = 0; i < labels.size(); i++) {
            NodeLabel nodeLabel = NodeLabel.of(labels.get(i));
            long labelId = elementIdentifierLabelTokenMapping.computeIfAbsent(nodeLabel, (l) -> {
                importer.labelTokenNodeLabelMapping.put(labelIdCounter, Collections.singletonList(nodeLabel));
                return labelIdCounter++;
            });
            labelIds[i] = labelId;
        }

        return labelIds;
    }

    private int processProperties(Result.ResultRow row, List<String> labels) {
        propertyImporter.registerPropertiesForLabels(labels);

        Map<String, Value> propertyValues = new HashMap<>();
        for (String propertyKey : propertyImporter.propertyColumns()) {
            Object value = CypherLoadingUtils.getProperty(row, propertyKey);
            if (value instanceof Number) {
                propertyValues.put(propertyKey, Values.of(value));
            } else if (value != null) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Unsupported type [%s] of value %s. Please use a numeric property.",
                    Values.of(value).valueGroup(),
                    value
                ));
            }
        }

        int propRef = cypherNodeProperties.size();
        cypherNodeProperties.add(propertyValues);

        return propRef;
    }

    private void reset() {
        buffer.reset();
        cypherNodeProperties.clear();
    }

    long rows() {
        return rows;
    }

    long maxId() {
        return maxNeoId;
    }

}
