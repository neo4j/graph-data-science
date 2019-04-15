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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.NullWeightMap;
import org.neo4j.graphalgo.core.WeightMap;

import java.util.Map;

public class Nodes {
    private final long offset;
    private final long rows;
    IdMap idMap;
    WeightMap nodeWeights;
    WeightMap nodeProps;
    private final Map<PropertyMapping, WeightMap> nodeProperties;
    private final double defaultNodeWeight;
    private final double defaultNodeValue;

    Nodes(
            long offset,
            long rows,
            IdMap idMap,
            WeightMap nodeWeights,
            WeightMap nodeProps,
            Map<PropertyMapping, WeightMap> nodeProperties,
            double defaultNodeWeight,
            double defaultNodeValue) {
        this.offset = offset;
        this.rows = rows;
        this.idMap = idMap;
        this.nodeWeights = nodeWeights;
        this.nodeProps = nodeProps;
        this.nodeProperties = nodeProperties;
        this.defaultNodeWeight = defaultNodeWeight;
        this.defaultNodeValue = defaultNodeValue;
    }

    public Map<PropertyMapping, WeightMap> nodeProperties() {
        return nodeProperties;
    }

    private WeightMapping nodeWeights() {
        if (nodeWeights != null) {
            return nodeWeights;
        }
        return new NullWeightMap(defaultNodeValue);
    }

    private WeightMapping nodeValues() {
        if (nodeProps != null) {
            return nodeProps;
        }
        return new NullWeightMap(defaultNodeWeight);
    }

    public long offset() {
        return offset;
    }

    long rows() {
        return rows;
    }
}
