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

import org.apache.commons.compress.utils.Sets;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphdb.Result;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private static final String ID_COLUMN = "id";
    static final String LABELS_COLUMN = "labels";
    static final Set<String> RESERVED_COLUMNS = Sets.newHashSet(ID_COLUMN, LABELS_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = Sets.newHashSet(ID_COLUMN);


    private long rows;
    private long maxNeoId = 0;
    private final Map<PropertyMapping, NodePropertiesBuilder> nodeProperties;
    private final NodesBatchBuffer buffer;
    private final List<Map<String, Number>> cypherNodeProperties;
    private final NodeImporter importer;
    private final boolean hasLabelInformation;

    private final Map<String, Long> labelIdMapping;
    private long labelIdCounter = 0;

    public NodeRowVisitor(Map<PropertyMapping, NodePropertiesBuilder> nodeProperties, NodesBatchBuffer buffer, NodeImporter importer, boolean hasLabelInformation) {
        this.nodeProperties = nodeProperties;
        this.buffer = buffer;
        this.importer = importer;
        this.cypherNodeProperties = new ArrayList<>(buffer.capacity());
        this.hasLabelInformation = hasLabelInformation;
        labelIdMapping = new HashMap<>();
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        long neoId = row.getNumber(ID_COLUMN).longValue();
        if (neoId > maxNeoId) {
            maxNeoId = neoId;
        }
        rows++;

        long[] labelIds = null;
        if (hasLabelInformation) {
            Object labelsObject = row.get(LABELS_COLUMN);
            if (!(labelsObject instanceof List)) {
                throw new IllegalArgumentException("dasd");
            }

            List<String> labelStrings = (List<String>) labelsObject;

            // Do not import this row if the record has no labels
            if (labelStrings.isEmpty()) {
                return true;
            }

            labelIds = new long[labelStrings.size()];

            for (int i = 0; i < labelStrings.size(); i++) {
                String labelString = labelStrings.get(i);
                long labelId = labelIdMapping.computeIfAbsent(labelString, (l) -> {
                    importer.labelProjectionMapping.put(labelIdCounter, Collections.singletonList(labelString));
                    return labelIdCounter++;
                });
                labelIds[i] = labelId;
            }
        }


        HashMap<String, Number> weights = new HashMap<>();
        for (Map.Entry<PropertyMapping, NodePropertiesBuilder> entry : nodeProperties.entrySet()) {
            PropertyMapping key = entry.getKey();
            Object value = CypherLoadingUtils.getProperty(row, entry.getKey().neoPropertyKey());
            if (value instanceof Number) {
                weights.put(key.propertyKey(), (Number) value);
            } else if (null == value) {
                weights.put(key.propertyKey(), key.defaultValue());
            } else {
                throw new IllegalArgumentException(String.format(
                        "Unsupported type [%s] of value %s. Please use a numeric property.",
                        Values.of(value).valueGroup(),
                        value));
            }
        }

        int propRef = cypherNodeProperties.size();
        cypherNodeProperties.add(weights);
        buffer.add(neoId, propRef, labelIds);
        if (buffer.isFull()) {
            flush();
            reset();
        }
        return true;
    }

    void flush() {
        importer.importCypherNodes(buffer, cypherNodeProperties);
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
