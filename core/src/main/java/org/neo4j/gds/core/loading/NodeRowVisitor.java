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

import org.neo4j.gds.core.loading.construction.NodeLabelToken;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.graphdb.Result;
import org.neo4j.values.storable.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final String ID_COLUMN = "id";
    private static final NodeLabelToken ALL_NODES_LABEL = NodeLabelTokens.of(ALL_NODES);

    static final String LABELS_COLUMN = "labels";
    static final Set<String> RESERVED_COLUMNS = Set.of(ID_COLUMN, LABELS_COLUMN);
    static final Set<String> REQUIRED_COLUMNS = Set.of(ID_COLUMN);

    private long rows;
    private long maxNeoId = 0;
    private RuntimeException error;

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
    public boolean visit(Result.ResultRow row) {
        long neoId = row.getNumber(ID_COLUMN).longValue();
        if (neoId > maxNeoId) {
            maxNeoId = neoId;
        }
        rows++;

        try {
            var labels = getLabels(row, neoId);
            var properties = getProperties(row);
            nodesBuilder.addNode(neoId, properties, labels);
        } catch (RuntimeException ex) {
            // Throwing an exception will not stop iterating the current result.
            // This might lead to a situation where followup exceptions shadow
            // the initial cause. If there is an exception, we immediately stop
            // the iteration and store the exception for consumption in the
            // CypherNodeLoader.
            this.error = ex;

            return false;
        }

        progressTracker.logProgress();
        return true;
    }

    Optional<RuntimeException> error() {
        return Optional.ofNullable(this.error);
    }

    private NodeLabelToken getLabels(Result.ResultRow row, long neoId) {
        if (hasLabelInformation) {
            Object labelsObject = row.get(LABELS_COLUMN);
            if (!(labelsObject instanceof List)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Type of column `%s` should be of type List, but was `%s`",
                    LABELS_COLUMN,
                    labelsObject
                ));
            }

            var labels = NodeLabelTokens.of(labelsObject);
            if (labels.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Node(%d) does not specify a label, but label column '%s' was specified.",
                    neoId,
                    LABELS_COLUMN
                ));
            }

            return labels;
        } else {
            return ALL_NODES_LABEL;
        }
    }

    private Map<String, Value> getProperties(Result.ResultRow row) {
        Map<String, Value> propertyValues = new HashMap<>();
        for (String propertyKey : propertyColumns) {
            Object valueObject = CypherLoadingUtils.getProperty(row, propertyKey);
            if (valueObject != null) {
                var value = ValueConverter.toValue(valueObject);
                propertyValues.put(propertyKey, value);
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
