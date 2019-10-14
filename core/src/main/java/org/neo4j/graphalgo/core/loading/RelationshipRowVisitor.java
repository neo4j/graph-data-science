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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.core.loading.RelationshipImporter.Imports;
import org.neo4j.graphalgo.core.loading.RelationshipImporter.PropertyReader;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

import java.util.Optional;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {

    private static final long NO_RELATIONSHIP_REFERENCE = -1L;

    private long lastSourceId = -1, lastTargetId = -1;
    private long source = -1, target = -1;
    private long rows = 0;
    private final RelationshipsBatchBuffer buffer;
    private final IdMap idMap;
    private final boolean hasRelationshipProperty;
    private final double defaultRelPropertyValue;
    private long relationshipCount;
    private final Imports imports;
    private final PropertyReader relPropertyReader;

    RelationshipRowVisitor(
            RelationshipsBatchBuffer buffer,
            IdMap idMap,
            Optional<Double> maybeDefaultRelProperty,
            Imports imports
    ) {
        this.buffer = buffer;
        this.idMap = idMap;
        this.hasRelationshipProperty = maybeDefaultRelProperty.isPresent();
        this.defaultRelPropertyValue = maybeDefaultRelProperty.orElseGet(PropertyMapping.EMPTY_PROPERTY::defaultValue);
        this.imports = imports;
        this.relPropertyReader = RelationshipImporter.preLoadedPropertyReader();
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;
        long sourceId = row.getNumber("source").longValue();
        if (sourceId != lastSourceId) {
            source = idMap.toMappedNodeId(sourceId);
            lastSourceId = sourceId;
        }
        if (source == -1) {
            return true;
        }
        long targetId = row.getNumber("target").longValue();
        if (targetId != lastTargetId) {
            target = idMap.toMappedNodeId(targetId);
            lastTargetId = targetId;
        }
        if (target == -1) {
            return true;
        }
        long longWeight = hasRelationshipProperty ? Double.doubleToLongBits(extractPropertyValue(row)) : -1L;
        buffer.add(
                source,
                target,
                NO_RELATIONSHIP_REFERENCE,
                longWeight
        );
        if (buffer.isFull()) {
            flush();
            reset();
        }
        return true;
    }

    void flush() {
        long imported = imports.importRels(buffer, relPropertyReader);
        relationshipCount += RawValues.getHead(imported);
    }

    private void reset() {
        buffer.reset();
    }

    private double extractPropertyValue(Result.ResultRow row) {
        Object property = CypherLoadingUtils.getProperty(row, "weight");
        return property instanceof Number ? ((Number) property).doubleValue() : defaultRelPropertyValue;
    }

    public long rows() {
        return rows;
    }

    public long relationshipCount() {
        return relationshipCount;
    }

}
