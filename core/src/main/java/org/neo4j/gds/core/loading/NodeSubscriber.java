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

import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.ErrorCachingQuerySubscriber;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValues;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class NodeSubscriber extends ErrorCachingQuerySubscriber {

    private static final String ID_COLUMN = "id";
    private static final int UNINITIALIZED = -1;

    static final String LABELS_COLUMN = "labels";
    static final Set<String> RESERVED_COLUMNS = Set.of(ID_COLUMN, LABELS_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = Set.of(ID_COLUMN);

    private final ProgressTracker progressTracker;

    private long rows;
    private long maxNeoId = 0;
    private NodesBuilder nodesBuilder;

    private long neoId = -1L;
    private SequenceValue labels = VirtualValues.EMPTY_LIST;
    private Map<String, Value> properties;

    private int idOffset = UNINITIALIZED;
    private int labelOffset = UNINITIALIZED;
    private String[] fieldNames;

    public NodeSubscriber(
        ProgressTracker progressTracker
    ) {
        this.progressTracker = progressTracker;
    }

    void initialize(String[] fieldNames, NodesBuilder nodesBuilder) {
        this.fieldNames = fieldNames;
        this.nodesBuilder = nodesBuilder;
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(ID_COLUMN)) {
                idOffset = i;
            }
            if (fieldNames[i].equals(LABELS_COLUMN)) {
                labelOffset = i;
            }
        }
    }

    long rows() {
        return rows;
    }

    long maxId() {
        return maxNeoId;
    }


    @Override
    public void onResult(int numberOfFields) {

    }

    @Override
    public void onRecord() {
       this.properties = new HashMap<>();
    }

    @Override
    public void onField(int offset, AnyValue value)  {
        if (offset == idOffset) {
            neoId = ((NumberValue) value).longValue();
            if (neoId > maxNeoId) {
                maxNeoId = neoId;
            }
        } else if (offset == labelOffset) {
            if (!(value.isSequenceValue())) {
                //throwing here will abort execution and error will appear in onError
                throw new IllegalArgumentException(formatWithLocale(
                    "Type of column `%s` should be of type List, but was `%s`",
                    LABELS_COLUMN,
                    value
                ));
            }

            labels = (SequenceValue) value;
        } else {//properties
            if ( value != Values.NO_VALUE) {
                properties.put(fieldNames[offset], ValueConverter.toValue(value));
            }
        }
    }

    @Override
    public void onRecordCompleted() {
        if (labelOffset >= 0 && labels.isEmpty()) {
            //throwing here will abort execution and error will appear in onError
            throw new IllegalArgumentException(formatWithLocale(
                "Node(%d) does not specify a label, but label column '%s' was specified.",
                neoId,
                LABELS_COLUMN
            ));
        } else if (labels.isEmpty()) {
            nodesBuilder.addNode(neoId, properties);
        } else {
            nodesBuilder.addNode(neoId, properties, NodeLabelTokens.of(labels));
        }
        rows++;
        progressTracker.logProgress();
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {
    }
}
