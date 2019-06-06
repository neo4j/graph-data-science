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
package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphdb.Result;

import java.util.concurrent.atomic.LongAdder;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private long lastSourceId = -1, lastTargetId = -1;
    private int source = -1, target = -1;
    private long rows = 0;
    private IntIdMap idMap;
    private boolean hasRelationshipWeights;
    private double defaultWeight;
    private AdjacencyMatrix matrix;
    private DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;
    private final LongAdder relationshipCount;

    RelationshipRowVisitor(
            IntIdMap idMap,
            boolean hasRelationshipWeights,
            double defaultWeight,
            AdjacencyMatrix matrix,
            DuplicateRelationshipsStrategy duplicateRelationshipsStrategy,
            LongAdder relationshipCount) {
        this.idMap = idMap;
        this.hasRelationshipWeights = hasRelationshipWeights;
        this.defaultWeight = defaultWeight;
        this.matrix = matrix;
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
        this.relationshipCount = relationshipCount;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;
        long sourceId = row.getNumber("source").longValue();
        if (sourceId != lastSourceId) {
            source = idMap.get(sourceId);
            lastSourceId = sourceId;
        }
        if (source == -1) {
            return true;
        }
        long targetId = row.getNumber("target").longValue();
        if (targetId != lastTargetId) {
            target = idMap.get(targetId);
            lastTargetId = targetId;
        }
        if (target == -1) {
            return true;
        }

        duplicateRelationshipsStrategy.handle(
                source,
                target,
                matrix,
                hasRelationshipWeights,
                defaultWeight,
                () -> extractWeight(row),
                relationshipCount
        );

        return true;
    }

    private Number extractWeight(Result.ResultRow row) {
        Object weight = CypherLoadingUtils.getProperty(row, "weight");
        return weight instanceof Number ? ((Number) weight).doubleValue() : null;
    }

    public long rows() {
        return rows;
    }


}
