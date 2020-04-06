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
package org.neo4j.graphalgo.api;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.ElementIdentifier;

import java.util.Map;

public class UnionNodeProperties implements NodeProperties {

    private final Map<ElementIdentifier, NodeProperties> labelToNodePropertiesMap;
    private final Map<ElementIdentifier, BitSet> elementIdentifierToBitSetMap;

    public UnionNodeProperties(Map<ElementIdentifier, NodeProperties> labelToNodePropertiesMap, Map<ElementIdentifier, BitSet> elementIdentifierToBitSetMap) {
        this.labelToNodePropertiesMap = labelToNodePropertiesMap;
        this.elementIdentifierToBitSetMap = elementIdentifierToBitSetMap;
    }

    @Override
    public double nodeProperty(long nodeId) {
        ElementIdentifier firstLabel = null;

        for (ElementIdentifier label : elementIdentifierToBitSetMap.keySet()) {
            if (elementIdentifierToBitSetMap.get(label).get(nodeId)) {
                firstLabel = label;
                break;
            }
        }

        if (firstLabel == null) {
            throw new IllegalArgumentException(String.format("Could not find label for node(%d)", nodeId));
        }

        if (labelToNodePropertiesMap.containsKey(firstLabel)) {
            return labelToNodePropertiesMap.get(firstLabel).nodeProperty(nodeId);
        } else {
            return Double.NaN;
        }
    }
}
