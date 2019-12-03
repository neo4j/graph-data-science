/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphalgo.core.utils.ParallelUtil.DEFAULT_BATCH_SIZE;

class MultiRelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final long NO_RELATIONSHIP_REFERENCE = -1L;
    private static final String SOURCE_COLUMN = "source";
    private static final String TARGET_COLUMN = "target";
    private static final String TYPE_COLUMN = "type";

    private final IdMap idMap;
    private final Map<String, Integer> propertyKeyIdsByName;
    private final Map<String, Double> propertyDefaultValueByName;
    private final CypherRelationshipsImporter.Context importerContext;
    private final int batchSize;
    private final int bufferSize;
    private final int propertyCount;

    private final Map<String, SingleTypeRelationshipImporter> localImporters;
    private final Map<String, RelationshipPropertiesBatchBuffer> localPropertiesBuffers;
    private final Map<String, Integer> localRelationshipIds;

    private long lastNeoSourceId = -1, lastNeoTargetId = -1;
    private long sourceId = -1, targetId = -1;
    private long rows = 0;
    private long relationshipCount;

    MultiRelationshipRowVisitor(
        IdMap idMap,
        CypherRelationshipsImporter.Context importerContext,
        Map<String, Integer> propertyKeyIdsByName,
        Map<String, Double> propertyDefaultValueByName,
        int batchSize,
        int bufferSize
    ) {
        this.idMap = idMap;
        this.propertyKeyIdsByName = propertyKeyIdsByName;
        this.propertyDefaultValueByName = propertyDefaultValueByName;
        this.propertyCount = propertyKeyIdsByName.size();
        this.importerContext = importerContext;
        this.batchSize = batchSize;
        this.bufferSize = bufferSize;
        this.localImporters = new HashMap<>();
        this.localPropertiesBuffers = new HashMap<>();
        this.localRelationshipIds = new HashMap<>();
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
        String relationshipType = row.getString(TYPE_COLUMN);

        if (!localImporters.containsKey(relationshipType)) {
            // Lazily init relationship importer builder
            SingleTypeRelationshipImporter.Builder.WithImporter importerBuilder = importerContext
                .getOrCreateImporterBuilder(relationshipType);
            // Create thread-local buffer for relationship properties
            RelationshipPropertiesBatchBuffer propertiesBuffer = new RelationshipPropertiesBatchBuffer(
                batchSize == CypherLoadingUtils.NO_BATCHING ? DEFAULT_BATCH_SIZE : batchSize,
                propertyCount
            );
            // Create thread-local relationship importer
            SingleTypeRelationshipImporter importer = importerBuilder.withBuffer(idMap, bufferSize, propertiesBuffer);

            localImporters.put(relationshipType, importer);
            localPropertiesBuffers.put(relationshipType, propertiesBuffer);
            localRelationshipIds.put(relationshipType, 0);
        }

        return visit(row, relationshipType);
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
        RelationshipPropertiesBatchBuffer propertiesBuffer = localPropertiesBuffers.get(relationshipType);
        int nextRelationshipId = localRelationshipIds.get(relationshipType);

        // We write source and target into
        // the buffer and add a reference
        // to the property batch buffer.
        importer.buffer().add(
            sourceId,
            targetId,
            NO_RELATIONSHIP_REFERENCE,
            nextRelationshipId
        );

        readPropertyValues(row, nextRelationshipId, propertiesBuffer);

        if (importer.buffer().isFull()) {
            flush(importer);
        }

        localRelationshipIds.put(relationshipType, nextRelationshipId + 1);

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
        propertyKeyIdsByName.forEach((propertyKey, propertyKeyId) -> {
            Object property = CypherLoadingUtils.getProperty(row, propertyKey);
            double propertyValue = property instanceof Number
                ? ((Number) property).doubleValue()
                : propertyDefaultValueByName.get(propertyKey);

            propertiesBuffer.add(relationshipId, propertyKeyId, propertyValue);
        });
    }

    private void flush(SingleTypeRelationshipImporter importer) {
        long imported = importer.importRels();
        relationshipCount += RawValues.getHead(imported);
        importer.buffer().reset();
        // TODO: maybe we need to reset the relationship property batch buffer as well
    }

    void flushAll() {
        relationshipCount += localImporters.values().stream()
            .map(SingleTypeRelationshipImporter::importRels)
            .map(RawValues::getHead)
            .reduce(Integer::sum)
            .orElse(0);
    }

}
