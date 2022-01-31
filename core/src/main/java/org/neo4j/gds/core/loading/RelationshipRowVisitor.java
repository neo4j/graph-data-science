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

import org.eclipse.collections.impl.map.mutable.primitive.ObjectDoubleHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.graphdb.Result;

import java.util.Set;

import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.utils.ExceptionUtil.validateSourceNodeIsLoaded;
import static org.neo4j.gds.utils.ExceptionUtil.validateTargetNodeIsLoaded;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final String SOURCE_COLUMN = "source";
    private static final String TARGET_COLUMN = "target";
    static final String TYPE_COLUMN = "type";
    static final Set<String> REQUIRED_COLUMNS = Set.of(SOURCE_COLUMN, TARGET_COLUMN);
    static final Set<String> RESERVED_COLUMNS = Set.of(SOURCE_COLUMN, TARGET_COLUMN, TYPE_COLUMN);

    private final IdMap idMap;
    private final ObjectIntHashMap<String> propertyKeyIdsByName;
    private final ObjectDoubleHashMap<String> propertyDefaultValueByName;
    private final CypherRelationshipLoader.Context loaderContext;
    private final ProgressTracker progressTracker;
    private final boolean noProperties;
    private final boolean singleProperty;
    private final String singlePropertyKey;
    private final double[] propertyValueBuffer;
    private final boolean isAnyRelTypeQuery;
    private final boolean throwOnUnMappedNodeIds;

    private long lastNeoSourceId = IdMap.NOT_FOUND, lastNeoTargetId = IdMap.NOT_FOUND;
    private long sourceId = IdMap.NOT_FOUND, targetId = IdMap.NOT_FOUND;
    private long rows = 0;

    RelationshipRowVisitor(
        IdMap idMap,
        CypherRelationshipLoader.Context loaderContext,
        ObjectIntHashMap<String> propertyKeyIdsByName,
        ObjectDoubleHashMap<String> propertyDefaultValueByName,
        boolean isAnyRelTypeQuery,
        boolean throwOnUnMappedNodeIds,
        ProgressTracker progressTracker
    ) {
        this.idMap = idMap;
        this.propertyKeyIdsByName = propertyKeyIdsByName;
        this.propertyDefaultValueByName = propertyDefaultValueByName;
        int propertyCount = propertyKeyIdsByName.size();
        this.noProperties = propertyCount == 0;
        this.singleProperty = propertyCount == 1;
        this.propertyValueBuffer = new double[propertyCount];

        this.progressTracker = progressTracker;
        this.singlePropertyKey = propertyKeyIdsByName.keySet().stream().findFirst().orElse("");
        this.loaderContext = loaderContext;
        this.isAnyRelTypeQuery = isAnyRelTypeQuery;
        this.throwOnUnMappedNodeIds = throwOnUnMappedNodeIds;
    }

    public long rows() {
        return rows;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;

        var relationshipType = isAnyRelTypeQuery
            ? ALL_RELATIONSHIPS
            : RelationshipType.of(row.getString(TYPE_COLUMN));

        return visit(row, relationshipType);
    }

    private boolean visit(Result.ResultRow row, RelationshipType relationshipType) {
        readSourceId(row);
        readTargetId(row);

        if (!throwOnUnMappedNodeIds && (sourceId == IdMap.NOT_FOUND || targetId == IdMap.NOT_FOUND)) {
            return true;
        }

        var relationshipsBuilder = loaderContext.getOrCreateRelationshipsBuilder(relationshipType);

        if (noProperties) {
            relationshipsBuilder.addFromInternal(sourceId, targetId);
        } else if (singleProperty) {
            relationshipsBuilder.addFromInternal(sourceId, targetId, readPropertyValue(row, singlePropertyKey));
        } else {
            relationshipsBuilder.addFromInternal(sourceId, targetId, readPropertyValues(row));
        }

        progressTracker.logProgress();

        return true;
    }

    private void readSourceId(Result.ResultRow row) {
        long neoSourceId = row.getNumber(SOURCE_COLUMN).longValue();
        if (neoSourceId != lastNeoSourceId) {
            sourceId = idMap.toMappedNodeId(neoSourceId);
            if (throwOnUnMappedNodeIds) {
                validateSourceNodeIsLoaded(sourceId, neoSourceId);
            }
            lastNeoSourceId = neoSourceId;
        }
    }

    private void readTargetId(Result.ResultRow row) {
        long neoTargetId = row.getNumber(TARGET_COLUMN).longValue();
        if (neoTargetId != lastNeoTargetId) {
            targetId = idMap.toMappedNodeId(neoTargetId);
            if (throwOnUnMappedNodeIds) {
                validateTargetNodeIsLoaded(targetId, neoTargetId);
            }
            lastNeoTargetId = neoTargetId;
        }
    }

    private double[] readPropertyValues(Result.ResultRow row) {
        propertyKeyIdsByName.forEachKeyValue((propertyKey, propertyKeyId) ->
            propertyValueBuffer[propertyKeyId] = readPropertyValue(row, propertyKey)
        );
        return propertyValueBuffer;
    }

    private double readPropertyValue(Result.ResultRow row, String propertyKey) {
        Object property = CypherLoadingUtils.getProperty(row, propertyKey);
        return property instanceof Number
            ? ((Number) property).doubleValue()
            : propertyDefaultValueByName.get(propertyKey);
    }
}
