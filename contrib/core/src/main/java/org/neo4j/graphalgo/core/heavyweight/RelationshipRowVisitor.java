/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

class RelationshipRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private long lastSourceId = -1, lastTargetId = -1;
    private int source = -1, target = -1;
    private long rows = 0;
    private IdMap idMap;
    private boolean hasRelationshipWeights;
    private WeightMap relWeights;
    private AdjacencyMatrix matrix;
    private DuplicateRelationshipsStrategy duplicateRelationshipsStrategy;

    RelationshipRowVisitor(IdMap idMap, boolean hasRelationshipWeights, WeightMap relWeights, AdjacencyMatrix matrix, DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        this.idMap = idMap;
        this.hasRelationshipWeights = hasRelationshipWeights;
        this.relWeights = relWeights;
        this.matrix = matrix;
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
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

        duplicateRelationshipsStrategy.handle(source, target, matrix, hasRelationshipWeights, relWeights, () -> extractWeight(row));

        return true;
    }

    private Number extractWeight(Result.ResultRow row) {
        Object weight = CypherLoadingUtils.getProperty(row, "weight");
        return weight instanceof  Number ? ((Number) weight).doubleValue() : null;
    }

    public long rows() {
        return rows;
    }


}
