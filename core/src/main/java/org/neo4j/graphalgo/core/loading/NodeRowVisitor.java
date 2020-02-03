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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private static final String ID_COLUMN = "id";
    static final Set<String> RESERVED_COLUMNS = Sets.newHashSet(ID_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = RESERVED_COLUMNS;

    private long rows;
    private long maxNeoId = 0;
    private Map<PropertyMapping, NodePropertiesBuilder> nodeProperties;
    private NodesBatchBuffer buffer;
    private List<Map<String, Number>> cypherNodeProperties;
    private NodeImporter importer;

    public NodeRowVisitor(Map<PropertyMapping, NodePropertiesBuilder> nodeProperties, NodesBatchBuffer buffer, NodeImporter importer) {
        this.nodeProperties = nodeProperties;
        this.buffer = buffer;
        this.importer = importer;
        this.cypherNodeProperties = new ArrayList<>(buffer.capacity());
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        long neoId = row.getNumber("id").longValue();
        if (neoId > maxNeoId) {
            maxNeoId = neoId;
        }
        rows++;

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
        buffer.add(neoId, propRef);
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
