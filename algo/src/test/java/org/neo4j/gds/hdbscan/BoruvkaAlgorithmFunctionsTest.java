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
package org.neo4j.gds.hdbscan;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BoruvkaAlgorithmFunctionsTest {

    @Test
    void singleComponentShouldWorkOnLeaf(){
        KdNode kdNode = KdNode.createLeaf(0, 0, 4, null);
        var kdTree =new KdTree(
            HugeLongArray.of(0,1,2,3),
            null,
            kdNode,
            1
        );

        var boruvkaMST =  BoruvkaMST.createWithZeroCores(
            null,
            kdTree,
            4,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER
        );

        assertThat(boruvkaMST.updateSingleComponent(kdNode)).isFalse();
        boruvkaMST.mergeComponents(0,1);
        boruvkaMST.mergeComponents(2,3);
        assertThat(boruvkaMST.updateSingleComponent(kdNode)).isFalse();
        boruvkaMST.mergeComponents(0,2);
        assertThat(boruvkaMST.updateSingleComponent(kdNode)).isTrue();

    }

    @Test
    void singleComponentOrShouldWork(){
        var kdTree =new KdTree(
            HugeLongArray.of(0,1,2,3),
            null,
            null,
            1
        );

        var boruvkaMST =  BoruvkaMST.createWithZeroCores(
            null,
            kdTree,
            4,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER
        );

        KdNode kdNode1 = KdNode.createLeaf(1, 1, 2, null);
        KdNode kdNode2 = KdNode.createLeaf(2, 2, 4, null);

        boruvkaMST.updateSingleComponent(kdNode1);
        boruvkaMST.updateSingleComponent(kdNode2);

        assertThat(boruvkaMST.singleComponentOr(kdNode1,-1)).isEqualTo(1);
        assertThat(boruvkaMST.singleComponentOr(kdNode2,-2)).isEqualTo(-2);

        boruvkaMST.mergeComponents(2,3);
        boruvkaMST.updateSingleComponent(kdNode2);

        assertThat(boruvkaMST.singleComponentOr(kdNode2,-3)).isIn(2L,3L);

    }

    @Test
    void singleComponentShouldWorkOnSplitNode(){
        KdNode kdNode = KdNode.createLeaf(0, 0, 8, null);
        KdNode left = KdNode.createLeaf(0, 0, 4, null);
        KdNode right = KdNode.createLeaf(0, 4, 8, null);

        kdNode.leftChild(left);
        kdNode.rightChild(right);

        var kdTree =new KdTree(
            HugeLongArray.of(0,1,2,3,4,5,6,7),
            null,
            kdNode,
            1
        );

        var boruvkaMST =  BoruvkaMST.createWithZeroCores(
            null,
            kdTree,
            8,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER
        );

        assertThat(boruvkaMST.updateSingleComponent(kdNode)).isFalse();
        boruvkaMST.mergeComponents(0,1);
        boruvkaMST.mergeComponents(2,3);
        boruvkaMST.mergeComponents(0,2);

        assertThat(boruvkaMST.updateSingleComponent(kdNode)).isFalse();
        assertThat(boruvkaMST.updateSingleComponent(left)).isTrue(); //as a byproduct

        boruvkaMST.mergeComponents(4,5);
        boruvkaMST.mergeComponents(6,7);
        boruvkaMST.mergeComponents(4,6);

        assertThat(boruvkaMST.updateSingleComponent(right)).isTrue();
        assertThat(boruvkaMST.updateSingleComponent(kdNode)).isTrue();

    }

    @Test
    void baseCaseShouldWork(){
        DoubleArrayNodePropertyValues nodeProps=new DoubleArrayNodePropertyValues() {
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{nodeId};
            }

            @Override
            public long nodeCount() {
                return 0;
            }
        };

        var kdTree = mock(KdTree.class);

        var coreResult = mock(CoreResult.class);
        when(coreResult.createCoreArray()).thenReturn(HugeDoubleArray.of(0,0,10,10,0,0,0,0,0,0));
        when(coreResult.neighboursOf(anyLong())).thenReturn(new Neighbour[0]);

        var boruvkaMST =  BoruvkaMST.create(
            nodeProps,
            kdTree,
            coreResult,
            8,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER
        );

        assertThat(boruvkaMST.baseCase(0,1,nodeProps.doubleArrayValue(0),0)).isEqualTo(1); //distance
        assertThat(boruvkaMST.baseCase(2,3,nodeProps.doubleArrayValue(2),2)).isEqualTo(10); //corevalue
        assertThat(boruvkaMST.baseCase(2,6,nodeProps.doubleArrayValue(2),2)).isEqualTo(-1); //irrelevant


    }

    @Test
    void baseCaseShouldIgnoreSameComponents(){
        DoubleArrayNodePropertyValues nodeProps=mock(DoubleArrayNodePropertyValues.class);
        var kdTree = mock(KdTree.class);

        var coreResult = mock(CoreResult.class);
        when(coreResult.createCoreArray()).thenReturn(HugeDoubleArray.of(0,0,10,10));
        when(coreResult.neighboursOf(anyLong())).thenReturn(new Neighbour[0]);

        var boruvkaMST =  BoruvkaMST.create(
            nodeProps,
            kdTree,
            coreResult,
            8,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER
        );

        assertThat(boruvkaMST.baseCase(0,1,nodeProps.doubleArrayValue(0),1)).isEqualTo(-1); //distance

    }

    @Test
    void shouldPruneProperly(){

        DoubleArrayNodePropertyValues nodeProps=mock(DoubleArrayNodePropertyValues.class);
        var kdRoot = KdNode.createLeaf(0,0,2,mock(AABB.class));
        var kdTree =new KdTree(HugeLongArray.of(0,1,2),nodeProps,kdRoot,1);
        var boruvkaMST =  BoruvkaMST.createWithZeroCores(
            nodeProps,
            kdTree,
            3,
            new Concurrency(1),
            ProgressTracker.NULL_TRACKER
        );

        //prune based on distance
        boruvkaMST.tryUpdate(2,2,3,5);
        assertThat(boruvkaMST.prune(kdRoot,2,100)).isTrue();
        assertThat(boruvkaMST.prune(kdRoot,2,4)).isFalse();

        //prune based on component
        assertThat(boruvkaMST.prune(kdRoot,0,1)).isFalse();
        boruvkaMST.mergeComponents(0,1);
        boruvkaMST.updateSingleComponent(kdRoot);
        assertThat(boruvkaMST.prune(kdRoot,0,1)).isTrue();

    }
}
