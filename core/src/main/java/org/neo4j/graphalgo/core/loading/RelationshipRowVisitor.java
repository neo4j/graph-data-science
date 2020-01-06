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
import org.eclipse.collections.impl.map.mutable.primitive.ObjectDoubleHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final long NO_RELATIONSHIP_REFERENCE = -1L;
    private static final String SOURCE_COLUMN = "source";
    private static final String TARGET_COLUMN = "target";
    static final String TYPE_COLUMN = "type";
    static final Set<String> RESERVED_COLUMNS = Sets.newHashSet(SOURCE_COLUMN, TARGET_COLUMN, TYPE_COLUMN);

    private final IdMap idMap;
    private final ObjectIntHashMap<String> propertyKeyIdsByName;
    private final ObjectDoubleHashMap<String> propertyDefaultValueByName;
    private final CypherRelationshipLoader.Context loaderContext;
    private final int bufferSize;
    private final int propertyCount;
    private final boolean noProperties;
    private final boolean singleProperty;
    private final boolean multipleProperties;
    private final String singlePropertyKey;

    private final Map<String, SingleTypeRelationshipImporter> localImporters;
    private final Map<String, RelationshipPropertiesBatchBuffer> localPropertiesBuffers;
    private final ObjectIntHashMap<String> localRelationshipIds;
    private final boolean isAnyRelTypeQuery;

    private long lastNeoSourceId = -1, lastNeoTargetId = -1;
    private long sourceId = -1, targetId = -1;
    private long rows = 0;
    private long relationshipCount;

    RelationshipRowVisitor(
        IdMap idMap,
        CypherRelationshipLoader.Context loaderContext,
        ObjectIntHashMap<String> propertyKeyIdsByName,
        ObjectDoubleHashMap<String> propertyDefaultValueByName,
        int bufferSize,
        boolean isAnyRelTypeQuery
    ) {
        this.idMap = idMap;
        this.propertyKeyIdsByName = propertyKeyIdsByName;
        this.propertyDefaultValueByName = propertyDefaultValueByName;
        this.propertyCount = propertyKeyIdsByName.size();
        this.noProperties = propertyCount == 0;
        this.singleProperty = propertyCount == 1;
        this.multipleProperties = propertyCount > 1;
        this.singlePropertyKey = propertyKeyIdsByName.keySet().stream().findFirst().orElse("");
        this.loaderContext = loaderContext;
        this.bufferSize = bufferSize;
        this.localImporters = new HashMap<>();
        this.localPropertiesBuffers = new HashMap<>();
        this.localRelationshipIds = new ObjectIntHashMap<>();
        this.isAnyRelTypeQuery = isAnyRelTypeQuery;
    }

    public long rows() {
        return rows;
    }

    public long relationshipCount() {
        return relationshipCount;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;

        RelationshipTypeMapping relationshipType = isAnyRelTypeQuery
            ? RelationshipTypeMapping.all()
            : RelationshipTypeMapping.of(row.getString(TYPE_COLUMN), -1);

        String relationshipTypeName = relationshipType.typeName();

        if (!localImporters.containsKey(relationshipTypeName)) {
            // Lazily init relationship importer builder
            SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder = loaderContext
                .getOrCreateImporterBuilder(relationshipType);

            RelationshipImporter.PropertyReader propertyReader;

            if (multipleProperties) {
                // Create thread-local buffer for relationship properties
                RelationshipPropertiesBatchBuffer propertiesBuffer = new RelationshipPropertiesBatchBuffer(
                    bufferSize,
                    propertyCount
                );
                propertyReader = propertiesBuffer;
                localPropertiesBuffers.put(relationshipTypeName, propertiesBuffer);
            } else {
                // Single properties can be in-lined in the relationship batch
                propertyReader = RelationshipImporter.preLoadedPropertyReader();
            }
            // Create thread-local relationship importer
            SingleTypeRelationshipImporter importer = importerBuilder.withBuffer(idMap, bufferSize, propertyReader);

            localImporters.put(relationshipTypeName, importer);
            localRelationshipIds.put(relationshipTypeName, 0);
        }

        return visit(row, relationshipTypeName);
    }

    private boolean visit(Result.ResultRow row, String relationshipType) {

        readSourceId(row);
        if (sourceId == -1) {
            return true;
        }
        readTargetId(row);
        if (targetId == -1) {
            return true;
        }

        SingleTypeRelationshipImporter importer = localImporters.get(relationshipType);

        if (noProperties) {
            importer.buffer().add(
                sourceId,
                targetId,
                NO_RELATIONSHIP_REFERENCE
            );
        } else if (singleProperty) {
            importer.buffer().add(
                sourceId,
                targetId,
                NO_RELATIONSHIP_REFERENCE,
                Double.doubleToLongBits(readPropertyValue(row, singlePropertyKey))
            );
        } else {
            // Instead of inlining the property
            // value, we write a reference into
            // the properties batch buffer.
            int nextRelationshipId = localRelationshipIds.get(relationshipType);
            importer.buffer().add(
                sourceId,
                targetId,
                NO_RELATIONSHIP_REFERENCE,
                nextRelationshipId
            );
            readPropertyValues(row, nextRelationshipId, localPropertiesBuffers.get(relationshipType));
            localRelationshipIds.put(relationshipType, nextRelationshipId + 1);
        }

        if (importer.buffer().isFull()) {
            flush(importer);
            reset(relationshipType, importer);
        }

        return true;
    }

    private void readTargetId(Result.ResultRow row) {
        long neoTargetId = row.getNumber(TARGET_COLUMN).longValue();
        if (neoTargetId != lastNeoTargetId) {
            targetId = idMap.toMappedNodeId(neoTargetId);
            lastNeoTargetId = neoTargetId;
        }
    }

    private void readSourceId(Result.ResultRow row) {
        long neoSourceId = row.getNumber(SOURCE_COLUMN).longValue();
        if (neoSourceId != lastNeoSourceId) {
            sourceId = idMap.toMappedNodeId(neoSourceId);
            lastNeoSourceId = neoSourceId;
        }
    }

    private void readPropertyValues(Result.ResultRow row, int relationshipId, RelationshipPropertiesBatchBuffer propertiesBuffer) {
        propertyKeyIdsByName.forEachKeyValue((propertyKey, propertyKeyId) -> {
            propertiesBuffer.add(relationshipId, propertyKeyId, readPropertyValue(row, propertyKey));
        });
    }

    private double readPropertyValue(Result.ResultRow row, String propertyKey) {
        Object property = CypherLoadingUtils.getProperty(row, propertyKey);
        return property instanceof Number
            ? ((Number) property).doubleValue()
            : propertyDefaultValueByName.get(propertyKey);
    }

    private void flush(SingleTypeRelationshipImporter importer) {
        long imported = importer.importRels();
        relationshipCount += RawValues.getHead(imported);
    }

    private void reset(String relationshipType, SingleTypeRelationshipImporter importer) {
        importer.buffer().reset();
        localRelationshipIds.put(relationshipType, 0);
    }

    void flushAll() {
        relationshipCount += localImporters.values().stream()
            .mapToLong(SingleTypeRelationshipImporter::importRels)
            .mapToInt(RawValues::getHead)
            .sum();
    }

}
