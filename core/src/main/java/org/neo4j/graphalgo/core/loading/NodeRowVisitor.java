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

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphdb.Result;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.NodeLabel.ALL_NODES;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private static final String ID_COLUMN = "id";
    static final String LABELS_COLUMN = "labels";
    static final Set<String> RESERVED_COLUMNS = Set.of(ID_COLUMN, LABELS_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = Set.of(ID_COLUMN);

    private long rows;
    private long maxNeoId = 0;

    private final NodesBuilder nodesBuilder;
    private final Collection<String> propertyColumns;
    private final boolean hasLabelInformation;

    public NodeRowVisitor(
        NodesBuilder nodesBuilder,
        Collection<String> propertyColumns,
        boolean hasLabelInformation
    ) {
        this.nodesBuilder = nodesBuilder;
        this.propertyColumns = propertyColumns;
        this.hasLabelInformation = hasLabelInformation;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        long neoId = row.getNumber(ID_COLUMN).longValue();
        if (neoId > maxNeoId) {
            maxNeoId = neoId;
        }
        rows++;

        var labels = getLabels(row, neoId).toArray(NodeLabel[]::new);
        var properties = getProperties(row);
        nodesBuilder.addNode(neoId, properties, labels);
        return true;
    }

    private List<NodeLabel> getLabels(Result.ResultRow row, long neoId) {
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

            return labels.stream().map(NodeLabel::of).collect(Collectors.toList());
        } else {
            return List.of(ALL_NODES);
        }
    }

    private Map<String, Value> getProperties(Result.ResultRow row) {
        Map<String, Value> propertyValues = new HashMap<>();
        for (String propertyKey : propertyColumns) {
            Object valueObject = CypherLoadingUtils.getProperty(row, propertyKey);
            if (valueObject != null) {
                var value = Values.of(valueObject);
                if (value.valueGroup() == ValueGroup.NUMBER || value.valueGroup() == ValueGroup.NUMBER_ARRAY) {
                    propertyValues.put(propertyKey, value);
                } else {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Unsupported type [%s] of value %s. Please use a numeric property.",
                        value.valueGroup(),
                        value
                    ));
                }
            }
        }

        return propertyValues;
    }

    long rows() {
        return rows;
    }

    long maxId() {
        return maxNeoId;
    }

}
