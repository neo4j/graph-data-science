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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.function.LongToDoubleFunction;

public class StrongPruning {

    private final TreeStructure treeStructure;
    private final BitSet activeOriginalNodes;
    private final LongToDoubleFunction prizes;
    private final HugeLongArray parentArray;
    private final HugeDoubleArray parentCostArray;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    public StrongPruning(TreeStructure treeStructure,
        BitSet activeUnprunedOriginalNodes,
        LongToDoubleFunction prizes,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.treeStructure = treeStructure;
        this.activeOriginalNodes = activeUnprunedOriginalNodes;
        this.prizes = prizes;
        this.parentArray = HugeLongArray.newArray(treeStructure.originalNodeCount());
        this.parentCostArray = HugeDoubleArray.newArray(treeStructure.originalNodeCount());
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
        parentArray.fill(PrizeSteinerTreeResult.PRUNED);

    }

    void performPruning(){

        progressTracker.beginSubTask("Pruning Phase");
        if (activeOriginalNodes.cardinality() ==1){
            var singleActiveNode = activeOriginalNodes.nextSetBit(0);
            parentArray.set(singleActiveNode, PrizeSteinerTreeResult.ROOT);
        }
        else {
            HugeLongArray queue = HugeLongArray.newArray(activeOriginalNodes.cardinality());
            HugeDoubleArray dp = HugeDoubleArray.newArray(treeStructure.originalNodeCount());
            long totalPos = 0;
            long currentPos = 0;

            long bestSolutionIndex = -1;
            var tree = treeStructure.tree();
            var degrees = treeStructure.degrees();

            for (long u = 0; u < degrees.size(); ++u) {
                if (degrees.get(u) == 1) {
                    queue.set(totalPos++, u);
                }
            }

            while (currentPos < totalPos) {
                terminationFlag.assertRunning();;
                var nextLeaf = queue.get(currentPos++);
                var parent = new MutableLong(-1);
                var parentCost = new MutableDouble(-1);
                dp.addTo(nextLeaf, prizes.applyAsDouble(nextLeaf));
                degrees.set(nextLeaf, 0);
                progressTracker.logProgress();

                tree.forEachRelationship(nextLeaf, 1.0, (s, t, w) -> {
                    if (degrees.get(t) > 0) {
                        parent.set(t);
                        parentCost.setValue(w);
                        return false;
                    }
                    return true;
                });

                var actualParent = parent.get();

                parentArray.set(nextLeaf, PrizeSteinerTreeResult.ROOT);
                if (bestSolutionIndex == -1 || Double.compare(dp.get(bestSolutionIndex), dp.get(nextLeaf))< 0 ){
                    bestSolutionIndex = nextLeaf;
                }

                if (actualParent == -1) {
                    continue;
                }
                var actualParentCost = parentCost.getValue();

                if (Double.compare(actualParentCost, (dp.get(nextLeaf))) < 0) {
                    dp.addTo(actualParent, dp.get(nextLeaf) - actualParentCost);
                    parentArray.set(nextLeaf, actualParent);
                    parentCostArray.set(nextLeaf, actualParentCost);
                }

                degrees.addTo(actualParent, -1);
                if (degrees.get(actualParent) == 1) {
                    queue.set(totalPos++, actualParent);
                }
            }

            pruneUnnecessarySubTrees(bestSolutionIndex,queue,parentArray);

        }

        progressTracker.endSubTask("Pruning Phase");


    }

    void pruneUnnecessarySubTrees(long bestSolutionIndex, HugeLongArray helpingArray, HugeLongArray parentArray){
        for (long u=0;u<treeStructure.tree().nodeCount();++u){
            if (parentArray.get(u) == PrizeSteinerTreeResult.ROOT  && u!= bestSolutionIndex ){
                pruneSubtree(u,helpingArray,parentArray);
            }
        }
    }
    PrizeSteinerTreeResult resultTree(){
            return  new PrizeSteinerTreeResult(parentArray,parentCostArray);
    }

    private void pruneSubtree(long node, HugeLongArray helpingArray,HugeLongArray parents){
        terminationFlag.assertRunning();
        var tree = treeStructure.tree();
        long currentPosition = 0;
        MutableLong position = new MutableLong();
        helpingArray.set(position.getAndIncrement(),node);

        while (currentPosition < position.get()){
            var currentNode = helpingArray.get(currentPosition++);
            progressTracker.logProgress();

            parents.set(currentNode, PrizeSteinerTreeResult.PRUNED);

            tree.forEachRelationship(currentNode, (s,t)->{
                if (parents.get(t)==s){
                    helpingArray.set(position.getAndIncrement(),t);
                }
                return true;
            });
        }
    }

}
