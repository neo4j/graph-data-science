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
package gds.training;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelContext;

import java.util.Queue;

// only works because we run through all maxIterations
// does not terminate under normal circumstances
public class K1ColoringAdvancedExample implements PregelComputation {

    @Override
    public void compute(PregelContext context, long nodeId, Queue<Double> messages) {
        double nodeColor = context.getNodeValue(nodeId);
        double newColor;
        if (context.isInitialSuperStep()) {
            // In the first round, every node uses the same color: 0
            newColor = 0;
        } else {
            // We start by keeping our existing color
            newColor = nodeColor;
            if (messages != null) {
                // There are messages for us. We keep track of all the neighbor's colors
                // and choose the smallest one that is free
                BitSet neighborColors = new BitSet();
                Double message;
                while ((message = messages.poll()) != null && !message.isNaN()) {
                    neighborColors.set(message.longValue());
                }
                if (!neighborColors.isEmpty()) {
                    int possibleColor = 0;
                    int neighborColor;
                    BitSetIterator colors = neighborColors.iterator();
                    while ((neighborColor = colors.nextSetBit()) != -1) {
                        if (neighborColor == possibleColor) {
                            possibleColor++;
                        } else {
                            break;
                        }
                    }
                    newColor = possibleColor;
                }
            }
        }

        if (newColor != nodeColor) {
            context.setNodeValue(nodeId, newColor);
        }
        context.sendMessages(nodeId, newColor);
    }
}
