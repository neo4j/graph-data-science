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

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongLongMap;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

class ModularityColorArray {

    private final HugeLongArray sortedNodesByColor;
    private BitSet colorCoordinates;

    private final long numberOfColors;

    private ModularityColorArray(
        HugeLongArray sortedNodesByColor,
        BitSet colorCoordinates
    ) {
        this.colorCoordinates = colorCoordinates;
        this.sortedNodesByColor = sortedNodesByColor;
        this.numberOfColors = colorCoordinates.cardinality() - 1;
    }

    long getNumberOfColors() {
        return numberOfColors;
    }

    long getNextStartingCoordinate(long current) {
        return colorCoordinates.nextSetBit(current + 1);
    }


    long get(long indexId) {
        return sortedNodesByColor.get(indexId);
    }

    long getCount(long current) {
        return (getNextStartingCoordinate(current) - current);
    }

    void release() {
        sortedNodesByColor.release();
        colorCoordinates = null;
    }

    static ModularityColorArray createModularityColorArray(HugeLongArray colors, long nodeCount, BitSet usedColors) {
        var sortedNodesByColor = HugeLongArray.newArray(nodeCount);
        HugeLongArray colorCoordinateArray = HugeLongArray.newArray(usedColors.cardinality());
        LongLongMap colorToId = new LongLongHashMap();

        long encounteredColors = 0;

        BitSet setColorCoordinates = new BitSet(nodeCount + 1);

        long nextColorId = usedColors.nextSetBit(0);
        while (nextColorId != -1) {
            colorToId.put(nextColorId, encounteredColors++);
            nextColorId = usedColors.nextSetBit(nextColorId + 1);
        }
        for (long nodeId = 0; nodeId < nodeCount; ++nodeId) {
            long color = colors.get(nodeId);
            long colorId = colorToId.get(color);
            colorCoordinateArray.addTo(colorId, 1);
        }

        setColorCoordinates.set(0);

        long nodeSum = 0;
        for (long colorId = 0; colorId <= encounteredColors; ++colorId) {
            if (colorId == encounteredColors) {
                setColorCoordinates.set(nodeCount);
            } else {
                nodeSum += colorCoordinateArray.get(colorId);
                colorCoordinateArray.set(colorId, nodeSum);
                setColorCoordinates.set(nodeSum);
            }

        }
        for (long nodeId = nodeCount - 1; nodeId >= 0; --nodeId) {
            long color = colors.get(nodeId);
            long colorId = colorToId.get(color);
            long coordinate = colorCoordinateArray.get(colorId) - 1;
            sortedNodesByColor.set(coordinate, nodeId);
            colorCoordinateArray.set(colorId, coordinate);
        }
        return new ModularityColorArray(sortedNodesByColor, setColorCoordinates);
    }
}
