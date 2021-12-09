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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final String ID_COLUMN = "id";
    private static final NodeLabel[] ALL_NODES_LABEL_ARRAY = new NodeLabel[]{ ALL_NODES };

    static final String LABELS_COLUMN = "labels";
    static final Set<String> RESERVED_COLUMNS = Set.of(ID_COLUMN, LABELS_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = Set.of(ID_COLUMN);

    private long rows;
    private long maxNeoId = 0;

    private final NodesBuilder nodesBuilder;
    private final Collection<String> propertyColumns;
    private final boolean hasLabelInformation;
    private final ProgressTracker progressTracker;

    public NodeRowVisitor(
        NodesBuilder nodesBuilder,
        Collection<String> propertyColumns,
        boolean hasLabelInformation,
        ProgressTracker progressTracker
    ) {
        this.nodesBuilder = nodesBuilder;
        this.propertyColumns = propertyColumns;
        this.hasLabelInformation = hasLabelInformation;
        this.progressTracker = progressTracker;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        long neoId = row.getNumber(ID_COLUMN).longValue();
        if (neoId > maxNeoId) {
            maxNeoId = neoId;
        }
        rows++;

        var labels = getLabels(row, neoId);
        var properties = getProperties(row);
        nodesBuilder.addNode(neoId, properties, labels);
        progressTracker.logProgress();
        return true;
    }

    private NodeLabel[] getLabels(Result.ResultRow row, long neoId) {
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

            return nodeLabelArray(labels);
        } else {
            return ALL_NODES_LABEL_ARRAY;
        }
    }

    private Map<String, Value> getProperties(Result.ResultRow row) {
        Map<String, Value> propertyValues = new HashMap<>();
        for (String propertyKey : propertyColumns) {
            Object valueObject = CypherLoadingUtils.getProperty(row, propertyKey);
            if (valueObject != null) {
                var value = ValueUtils.of(valueObject);
                if (value.isSequenceValue()) {
                    var array = castToNumericArrayOrFail(value);
                    propertyValues.put(propertyKey, array);
                } else if (value instanceof Value) {
                    var storableValue = (Value) value;
                    if (storableValue.valueGroup() != ValueGroup.NUMBER) {
                        throw new IllegalArgumentException(formatWithLocale(
                            "Unsupported GDS node property of type `%s`.",
                            storableValue.getTypeName()
                        ));
                    }
                    propertyValues.put(propertyKey, storableValue);
                } else {
                    throw new IllegalArgumentException(formatWithLocale(
                        "Unsupported GDS node property of type `%s`.",
                        value.getTypeName()
                    ));
                }
            }
        }

        return propertyValues;
    }

    private ArrayValue castToNumericArrayOrFail(AnyValue value) {
        ArrayValue array = null;
        if (value instanceof ListValue) {
            var listValue = (ListValue) value;
            if (listValue.isEmpty()) {
                // encode as long array
                return Values.longArray(new long[0]);
            }
            // only 2 cases are valid here: list of double and list of long
            var firstValue = listValue.head();
            try {
                if (firstValue instanceof LongValue) {
                    var longArray = new long[listValue.size()];
                    var iterator = listValue.iterator();
                    for (int i = 0; i < listValue.size() && iterator.hasNext(); i++) {
                        longArray[i] = ((LongValue) iterator.next()).longValue();
                    }
                    array = Values.longArray(longArray);
                } else if (firstValue instanceof DoubleValue) {
                    var doubleArray = new double[listValue.size()];
                    var iterator = listValue.iterator();
                    for (int i = 0; i < listValue.size() && iterator.hasNext(); i++) {
                        doubleArray[i] = ((DoubleValue) iterator.next()).doubleValue();
                    }
                    array = Values.doubleArray(doubleArray);
                } else {
                    failOnBadList(listValue);
                }
            } catch (ClassCastException c) {
                failOnBadList(listValue);
            }
        } else {
             array = ((ArrayValue) value);
             if (array.valueGroup() != ValueGroup.NUMBER_ARRAY) {
                 failOnBadList(array);
             }
        }
        return array;
    }

    private void failOnBadList(AnyValue badList) {
        throw new IllegalArgumentException(formatWithLocale(
            "Only lists of uniformly typed numbers are supported as GDS node properties, but found an unsupported list `%s`.",
            badList
        ));
    }

    long rows() {
        return rows;
    }

    long maxId() {
        return maxNeoId;
    }

    private static NodeLabel[] nodeLabelArray(List<String> labels) {
        NodeLabel[] nodeLabels = new NodeLabel[labels.size()];
        for (int i = 0; i < labels.size(); i++) {
            String label = labels.get(i);
            nodeLabels[i] = NodeLabel.of(label);
        }
        return nodeLabels;
    }

}
