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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Result;

import java.util.Map;

class NodeRowVisitor implements Result.ResultVisitor<RuntimeException> {
    private long rows;
    private IntIdMap idMap;
    private Map<PropertyMapping, WeightMap> nodeProperties;

    NodeRowVisitor(IntIdMap idMap, Map<PropertyMapping, WeightMap> nodeProperties) {
        this.idMap = idMap;
        this.nodeProperties = nodeProperties;
    }

    @Override
    public boolean visit(Result.ResultRow row) throws RuntimeException {
        rows++;
        long id = row.getNumber("id").longValue();
        int graphId = idMap.add(id);

        for (Map.Entry<PropertyMapping, WeightMap> entry : nodeProperties.entrySet()) {
            Object value = CypherLoadingUtils.getProperty(row, entry.getKey().propertyKey);
            if (value instanceof Number) {
                // we need to store the weights as (source | target) encoding in our
                // non-huge relationship weights. Since we're storing properties for
                // nodes and not relationship, we only have the source available
                // and have to use -1 as the target id to signal that to the map
                // so that calls to nodeWeight(int) will be able to find this value again.
                entry.getValue().put(RawValues.combineIntInt(graphId, -1), ((Number) value).doubleValue());
            }
        }

        return true;
    }

    long rows() {
        return rows;
    }
}
