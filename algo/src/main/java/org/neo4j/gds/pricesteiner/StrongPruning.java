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
package org.neo4j.gds.pricesteiner;

import com.carrotsearch.hppc.BitSet;
import org.agrona.collections.MutableLong;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.function.LongToDoubleFunction;

public class StrongPruning {

    private final TreeStructure treeStructure;
    private final BitSet activeOriginalNodes;
    private final LongToDoubleFunction prizes;
    private final HugeLongArray parentArray;
    private final HugeDoubleArray parentCostArray;

    public StrongPruning(TreeStructure treeStructure, BitSet activeOriginalNodes, LongToDoubleFunction prizes) {
        this.treeStructure = treeStructure;
        this.activeOriginalNodes = activeOriginalNodes;
        this.prizes = prizes;
        this.parentArray = HugeLongArray.newArray(treeStructure.originalNodeCount());
        this.parentCostArray = HugeDoubleArray.newArray(treeStructure.originalNodeCount());
            parentArray.fill(PriceSteinerTreeResult.PRUNED);

    }

    void performPruning(){
        if (activeOriginalNodes.cardinality() ==1){
            var singleActiveNode = activeOriginalNodes.nextSetBit(0);
            parentArray.set(singleActiveNode,PriceSteinerTreeResult.ROOT);

        }
        else {
            HugeLongArray queue = HugeLongArray.newArray(activeOriginalNodes.capacity());
            HugeDoubleArray dp = HugeDoubleArray.newArray(treeStructure.originalNodeCount());
            long totalPos = 0;
            long currentPos = 0;
            var tree = treeStructure.tree();
            var degrees = treeStructure.degrees();

            for (long u = 0; u < degrees.size(); ++u) {
                if (degrees.get(u) == 1) {
                    queue.set(totalPos++, u);
                }

            }

            long rootNode = -1;
            while (currentPos < totalPos) {
                var nextLeaf = queue.get(currentPos++);
                rootNode = nextLeaf;
                var parent = new MutableLong(-1);
                var parentCost = new MutableDouble(-1);
                dp.addTo(nextLeaf, prizes.applyAsDouble(nextLeaf));
                degrees.set(nextLeaf, 0);

                tree.forEachRelationship(nextLeaf, 1.0, (s, t, w) -> {
                    if (degrees.get(t) > 0) {
                        parent.set(t);
                        parentCost.setValue(w);
                        return false;
                    }
                    return true;
                });
                var actualParent = parent.get();
                if (actualParent == -1) {
                    continue;
                }
                var actualParentCost = parentCost.getValue().doubleValue();

                parentArray.set(nextLeaf, actualParent);
                parentCostArray.set(nextLeaf, actualParentCost);

                if (Double.compare(actualParentCost, (dp.get(nextLeaf))) >= 0) {
                    setNodesAsInvalid(nextLeaf, queue, parent.get());
                } else {
                    dp.addTo(actualParent, dp.get(nextLeaf) - actualParentCost);
                }

                degrees.addTo(actualParent, -1);
                if (degrees.get(actualParent) == 1) {
                    queue.set(totalPos++, actualParent);
                }
            }
            parentArray.set(rootNode, PriceSteinerTreeResult.ROOT);
        }
    }

    PriceSteinerTreeResult resultTree(){
            return  new PriceSteinerTreeResult(parentArray,parentCostArray);
    }

    void setNodesAsInvalid(long startingNode, HugeLongArray helpingArray, long parentOfStart){
            var tree = treeStructure.tree();
            long currentPosition= 0;
            MutableLong position=new MutableLong();
            helpingArray.set(position.getAndIncrement(),startingNode);
            while (currentPosition < position.get()){
                var node = helpingArray.get(currentPosition++);
                activeOriginalNodes.clear(node);
                parentArray.set(node,PriceSteinerTreeResult.PRUNED);
                tree.forEachRelationship(node, (s,t)->{
                    if (t != parentOfStart && activeOriginalNodes.get(t)){
                        helpingArray.set(position.getAndIncrement(),t);
                    }
                    return true;
                });
            }
    }


}
