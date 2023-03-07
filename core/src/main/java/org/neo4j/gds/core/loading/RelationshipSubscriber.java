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

import org.eclipse.collections.api.map.primitive.ObjectDoubleMap;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.TextValue;

import java.util.Optional;
import java.util.Set;

import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.core.loading.LoadingExceptions.validateSourceNodeIsLoaded;
import static org.neo4j.gds.core.loading.LoadingExceptions.validateTargetNodeIsLoaded;

class RelationshipSubscriber implements QuerySubscriber {

    private static final String SOURCE_COLUMN = "source";
    private static final String TARGET_COLUMN = "target";
    private static final int UNINITIALIZED = -1;

    static final String TYPE_COLUMN = "type";
    static final Set<String> REQUIRED_COLUMNS = Set.of(SOURCE_COLUMN, TARGET_COLUMN);
    static final Set<String> RESERVED_COLUMNS = Set.of(SOURCE_COLUMN, TARGET_COLUMN, TYPE_COLUMN);

    private final IdMap idMap;
    private final CypherRelationshipLoader.Context loaderContext;
    private final ProgressTracker progressTracker;
    private double[] propertyValueBuffer;
    private double[] defaultValues;
    private final boolean throwOnUnMappedNodeIds;

    private int sourceOffset = UNINITIALIZED;
    private int targetOffset = UNINITIALIZED;
    private int typeOffset = UNINITIALIZED;

    private long lastNeoSourceId = IdMap.NOT_FOUND, lastNeoTargetId = IdMap.NOT_FOUND;
    private long sourceId = IdMap.NOT_FOUND, targetId = IdMap.NOT_FOUND;
    private long rows = 0;
    private RelationshipType relationshipType = ALL_RELATIONSHIPS;
    private int propertyIndex = 0;

    private RelationshipsBuilder allRelationshipsBuilder;

    private Optional<RuntimeException> error = Optional.empty();

    RelationshipSubscriber(
        IdMap idMap,
        CypherRelationshipLoader.Context loaderContext,
        boolean throwOnUnMappedNodeIds,
        ProgressTracker progressTracker
    ) {
        this.idMap = idMap;
        this.progressTracker = progressTracker;
        this.loaderContext = loaderContext;
        this.throwOnUnMappedNodeIds = throwOnUnMappedNodeIds;
    }

    void initialize(String[] fieldNames, ObjectDoubleMap<String> propertyDefaultValueByName) {
        this.defaultValues = new double[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            switch (fieldNames[i]) {
                case SOURCE_COLUMN:
                    sourceOffset = i;
                    break;
                case TARGET_COLUMN:
                    targetOffset = i;
                    break;
                case TYPE_COLUMN:
                    typeOffset = i;
                    break;
                default:
                    defaultValues[i] = propertyDefaultValueByName.get(fieldNames[i]);
                    break;
            }
        }
        int propertyCount = fieldNames.length - 2;
        if (typeOffset != UNINITIALIZED) {
            propertyCount--;
        } else {
            //means that this is a AnyRelTypeQuery
            allRelationshipsBuilder = loaderContext.getOrCreateRelationshipsBuilder(RelationshipType.ALL_RELATIONSHIPS);
        }
        this.propertyValueBuffer = new double[propertyCount];
    }

    Optional<RuntimeException> error() {
        return error;
    }

    public long rows() {
        return rows;
    }


    @Override
    public void onResult(int numberOfFields) {
    }

    @Override
    public void onRecord() {
        propertyIndex = 0;
    }

    @Override
    public void onField(int offset, AnyValue value) {
        if (offset == sourceOffset) {
            onSourceNode((NumberValue) value);
        } else if (offset == targetOffset) {
            onTargetNode((NumberValue) value);
        } else if (offset == typeOffset) {
            relationshipType = RelationshipType.of(((TextValue) value).stringValue());
        } else {
            this.propertyValueBuffer[propertyIndex++] = readPropertyValue(value, offset);
        }
    }

    private void onTargetNode(NumberValue value) {
        long neoTargetId = value.longValue();
        if (neoTargetId != lastNeoTargetId) {
            targetId = idMap.toMappedNodeId(neoTargetId);
            if (throwOnUnMappedNodeIds) {
                validateTargetNodeIsLoaded(targetId, neoTargetId);
            }
            lastNeoTargetId = neoTargetId;
        }
    }

    private void onSourceNode(NumberValue value) {
        long neoSourceId = value.longValue();
        if (neoSourceId != lastNeoSourceId) {
            sourceId = idMap.toMappedNodeId(neoSourceId);
            if (throwOnUnMappedNodeIds) {
                validateSourceNodeIsLoaded(sourceId, neoSourceId);
            }
            lastNeoSourceId = neoSourceId;
        }
    }

    private double readPropertyValue(AnyValue value, int offset) {
        if (value instanceof NumberValue) {
            return ((NumberValue) value).doubleValue();
        } else {
           return defaultValues[offset];
        }
    }

    @Override
    public void onRecordCompleted() {
        rows++;
        if (!throwOnUnMappedNodeIds && (sourceId == IdMap.NOT_FOUND || targetId == IdMap.NOT_FOUND)) {
            return;
        }

        var relationshipsBuilder =
            allRelationshipsBuilder != null ? allRelationshipsBuilder : loaderContext.getOrCreateRelationshipsBuilder(relationshipType);
        if (propertyValueBuffer.length == 0) {
            relationshipsBuilder.addFromInternal(sourceId, targetId);
        } else if (propertyValueBuffer.length == 1) {
            relationshipsBuilder.addFromInternal(sourceId, targetId, propertyValueBuffer[0]);
        } else {
            relationshipsBuilder.addFromInternal(sourceId, targetId, propertyValueBuffer);
        }

        progressTracker.logProgress();
    }
    @Override
    public void onError(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            this.error = Optional.of((RuntimeException) throwable);
        } else if (throwable instanceof QueryExecutionKernelException) {
            this.error = Optional.of(((QueryExecutionKernelException) throwable).asUserException());
        } else {
            this.error = Optional.of(new RuntimeException(throwable));
        }
    }

    @Override
    public void onResultCompleted(QueryStatistics statistics) {

    }
}
