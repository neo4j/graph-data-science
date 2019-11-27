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

import org.neo4j.graphalgo.RelationshipTypeMapping;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class MultiRelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final long NO_RELATIONSHIP_REFERENCE = -1L;
    private static final String SOURCE_COLUMN = "source";
    private static final String TARGET_COLUMN = "target";
    private static final String TYPE_COLUMN = "type";

    private final IdMap idMap;
    private final Map<String, SingleTypeRelationshipImporter> relationshipImportersByType;
    private final RelationshipPropertyBatchBuffer[] relationshipPropertyBatchBuffers;
    private final Map<String, Integer> propertyKeyIdsByName;
    private final Map<String, Double> propertyDefaultValueByName;

    private long lastNeoSourceId = -1, lastNeoTargetId = -1;
    private long sourceId = -1, targetId = -1;
    private long rows = 0;
    private int relationshipId = 0;
    private long relationshipCount;

    MultiRelationshipRowVisitor(
        IdMap idMap,
        List<Context> relationshipImporters,
        Map<String, Integer> propertyKeyIdsByName,
        Map<String, Double> propertyDefaultValueByName
    ) {
        this.idMap = idMap;
        this.relationshipImportersByType = relationshipImporters.stream()
            .collect(Collectors.toMap(
                context -> context.relationshipTypeMapping().typeName(),
                Context::singleTypeRelationshipImporter
            ));

        relationshipPropertyBatchBuffers = relationshipImporters.stream()
            .map(Context::relationshipPropertyBatchBuffer)
            .toArray(RelationshipPropertyBatchBuffer[]::new);

        this.propertyKeyIdsByName = propertyKeyIdsByName;
        this.propertyDefaultValueByName = propertyDefaultValueByName;
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
        return visit(row, relationshipImportersByType.get(row.getString(TYPE_COLUMN)));
    }

    private boolean visit(Result.ResultRow row, SingleTypeRelationshipImporter importer) {
        readSourceId(row);
        if (sourceId == -1) {
            return true;
        }
        readTargetId(row);
        if (targetId == -1) {
            return true;
        }

        relationshipId++;

        // We write source and target into
        // the buffer and add a reference
        // to the property batch buffer.
        importer.buffer().add(
            sourceId,
            targetId,
            NO_RELATIONSHIP_REFERENCE,
            relationshipId
        );

        readPropertyValues(row, relationshipId);

        if (importer.buffer().isFull()) {
            flush(importer);
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

    private void readPropertyValues(Result.ResultRow row, int relationshipId) {
        propertyKeyIdsByName.forEach((propertyKey, propertyKeyId) -> {
            Object property = CypherLoadingUtils.getProperty(row, propertyKey);
            double propertyValue = property instanceof Number
                ? ((Number) property).doubleValue()
                : propertyDefaultValueByName.get(propertyKey);

            for (int i = 0; i < propertyKeyIdsByName.size(); i++) {
                relationshipPropertyBatchBuffers[i].add(relationshipId, propertyKeyId, propertyValue);
            }
        });
    }

    private void flush(SingleTypeRelationshipImporter importer) {
        long imported = importer.importRels();
        relationshipCount += RawValues.getHead(imported);
        importer.buffer().reset();
    }

    void flushAll() {
        relationshipCount += relationshipImportersByType.values().stream()
            .map(SingleTypeRelationshipImporter::importRels)
            .map(RawValues::getHead)
            .reduce(Integer::sum)
            .orElse(0);
    }

    static class Context {
        private final RelationshipTypeMapping relationshipTypeMapping;
        private final SingleTypeRelationshipImporter singleTypeRelationshipImporter;
        private final RelationshipPropertyBatchBuffer relationshipPropertyBatchBuffer;

        Context(
            RelationshipTypeMapping relationshipTypeMapping,
            SingleTypeRelationshipImporter singleTypeRelationshipImporter,
            RelationshipPropertyBatchBuffer relationshipPropertyBatchBuffer
        ) {
            this.relationshipTypeMapping = relationshipTypeMapping;
            this.singleTypeRelationshipImporter = singleTypeRelationshipImporter;
            this.relationshipPropertyBatchBuffer = relationshipPropertyBatchBuffer;
        }

        RelationshipTypeMapping relationshipTypeMapping() {
            return relationshipTypeMapping;
        }

        SingleTypeRelationshipImporter singleTypeRelationshipImporter() {
            return singleTypeRelationshipImporter;
        }

        RelationshipPropertyBatchBuffer relationshipPropertyBatchBuffer() {
            return relationshipPropertyBatchBuffer;
        }
    }

}
