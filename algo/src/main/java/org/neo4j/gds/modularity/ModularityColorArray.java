/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.modularity;

import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

class ModularityColorArray {

    private final HugeLongArray sortedNodesByColor;
    private final HugeLongArray colorCoordinates;
    private LongLongMap colorToId;

    ModularityColorArray(
        HugeLongArray sortedNodesByColor,
        HugeLongArray colorCoordinates,
        LongLongMap colorToId
    ) {
        this.colorCoordinates = colorCoordinates;
        this.colorToId = colorToId;
        this.sortedNodesByColor = sortedNodesByColor;
    }

    long getStartingCoordinate(long color) {
        return colorCoordinates.get(colorToId.get(color));
    }

    long getEndingCoordinate(long color) {
        return colorCoordinates.get(colorToId.get(color) + 1);
    }

    long get(long indexId) {
        return sortedNodesByColor.get(indexId);
    }

    long getCount(long color) {
        return getEndingCoordinate(color) - getStartingCoordinate(color);
    }

    static ModularityColorArray createModularityColorArray(HugeLongArray colors, long nodeCount) {
        var sortedNodesByColor = HugeLongArray.newArray(nodeCount);
        LongLongMap colorCount = new LongLongHashMap();
        LongLongMap colorToId = new LongLongHashMap();
        long encounteredColors = 0;
        for (long nodeId = 0; nodeId < nodeCount; ++nodeId) {
            long color = colors.get(nodeId);
            if (!colorToId.containsKey(color)) {
                colorToId.put(color, encounteredColors++);
            }
            long colorId = colorToId.get(color);
            colorCount.addTo(colorId, 1);
        }

        var colorCoordinates = HugeLongArray.newArray(encounteredColors + 1);
        colorCoordinates.set(0, 0);
        long nodeSum = 0;
        for (long colorId = 0; colorId <= encounteredColors; ++colorId) {
            if (colorId == encounteredColors) {
                colorCoordinates.set(colorId, nodeCount);
            } else {
                nodeSum += colorCount.get(colorId);
                colorCoordinates.set(colorId, nodeSum);
            }
        }
        for (long nodeId = nodeCount - 1; nodeId >= 0; --nodeId) {
            long color = colors.get(nodeId);
            long colorId = colorToId.get(color);
            long coordinate = colorCoordinates.get(colorId) - 1;
            sortedNodesByColor.set(coordinate, nodeId);
            colorCoordinates.set(colorId, coordinate);
        }
        return new ModularityColorArray(sortedNodesByColor, colorCoordinates, colorToId);
    }
}
