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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.huge.loader.RelationshipImporter.Imports;
import org.neo4j.graphalgo.core.huge.loader.RelationshipImporter.WeightReader;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private long lastSourceId = -1, lastTargetId = -1;
    private long source = -1, target = -1;
    private long rows = 0;
    private final RelationshipsBatchBuffer buffer;
    private final IdMap idMap;
    private final boolean hasRelationshipWeights;
    private final double defaultWeight;
    private long relationshipCount;
    private final Imports imports;
    private final WeightReader weightReader;

    RelationshipRowVisitor(
            RelationshipsBatchBuffer buffer,
            IdMap idMap,
            boolean hasRelationshipWeights,
            double defaultWeight,
            RelationshipImporter importer,
            Imports imports
    ) {
        this.buffer = buffer;
        this.idMap = idMap;
        this.hasRelationshipWeights = hasRelationshipWeights;
        this.defaultWeight = defaultWeight;
        this.imports = imports;
        this.weightReader = importer.cypherResultsBackedWeightReader();
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
        long longWeight = hasRelationshipWeights ? Double.doubleToLongBits(extractWeight(row)) : -1L;
        buffer.add(
                source,
                target,
                -1L,
                longWeight
        );
        if (buffer.isFull()) {
            flush();
            reset();
        }
        return true;
    }

    void flush() {
        long imported = imports.importRels(buffer, weightReader);
        relationshipCount += RawValues.getHead(imported);
    }

    private void reset() {
        buffer.reset();
    }

    private double extractWeight(Result.ResultRow row) {
        Object weight = CypherLoadingUtils.getProperty(row, "weight");
        return weight instanceof Number ? ((Number) weight).doubleValue() : defaultWeight;
    }

    public long rows() {
        return rows;
    }

    public long relationshipCount() {
        return relationshipCount;
    }

}
