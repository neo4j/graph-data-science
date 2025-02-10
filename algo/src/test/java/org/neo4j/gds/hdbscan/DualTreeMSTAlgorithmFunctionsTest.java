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
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DualTreeMSTAlgorithmFunctionsTest {

    @Test
    void singleComponentShouldWorkOnLeaf(){
        KdNode kdNode = KdNode.createLeaf(0, 0, 4, null);
        var kdTree =new KdTree(
            HugeLongArray.of(0,1,2,3),
            null,
            kdNode,
            1
        );

        var dualTreeMST =  DualTreeMSTAlgorithm.createWithZeroCores(null,kdTree,4);

        assertThat(dualTreeMST.updateSingleComponent(kdNode)).isFalse();
        dualTreeMST.mergeComponents(0,1);
        dualTreeMST.mergeComponents(2,3);
        assertThat(dualTreeMST.updateSingleComponent(kdNode)).isFalse();
        dualTreeMST.mergeComponents(0,2);
        assertThat(dualTreeMST.updateSingleComponent(kdNode)).isTrue();

    }

    @Test
    void singleComponentOrShouldWork(){
        var kdTree =new KdTree(
            HugeLongArray.of(0,1,2,3),
            null,
            null,
            1
        );
        var dualTreeMST =  DualTreeMSTAlgorithm.createWithZeroCores(null,kdTree,4);
        KdNode kdNode1 = KdNode.createLeaf(1, 1, 2, null);
        KdNode kdNode2 = KdNode.createLeaf(2, 2, 4, null);

        dualTreeMST.updateSingleComponent(kdNode1);
        dualTreeMST.updateSingleComponent(kdNode2);

        assertThat(dualTreeMST.singleComponentOr(kdNode1,-1)).isEqualTo(1);
        assertThat(dualTreeMST.singleComponentOr(kdNode2,-2)).isEqualTo(-2);

        dualTreeMST.mergeComponents(2,3);
        dualTreeMST.updateSingleComponent(kdNode2);

        assertThat(dualTreeMST.singleComponentOr(kdNode2,-3)).isIn(2L,3L);

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

        var dualTreeMST =  DualTreeMSTAlgorithm.createWithZeroCores(null,kdTree,8);

        assertThat(dualTreeMST.updateSingleComponent(kdNode)).isFalse();
        dualTreeMST.mergeComponents(0,1);
        dualTreeMST.mergeComponents(2,3);
        dualTreeMST.mergeComponents(0,2);

        assertThat(dualTreeMST.updateSingleComponent(kdNode)).isFalse();
        assertThat(dualTreeMST.updateSingleComponent(left)).isTrue(); //as a byproduct

        dualTreeMST.mergeComponents(4,5);
        dualTreeMST.mergeComponents(6,7);
        dualTreeMST.mergeComponents(4,6);

        assertThat(dualTreeMST.updateSingleComponent(right)).isTrue();
        assertThat(dualTreeMST.updateSingleComponent(kdNode)).isTrue();

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
        when(coreResult.createCoreArray()).thenReturn(HugeDoubleArray.of(0,0,10,10));
        when(coreResult.neighboursOf(anyLong())).thenReturn(new Neighbour[0]);

        var dualTreeMST =  DualTreeMSTAlgorithm.create(nodeProps,kdTree, coreResult,8);

        assertThat(dualTreeMST.baseCase(0,1,0, nodeProps.doubleArrayValue(0))).isEqualTo(1); //distance
        assertThat(dualTreeMST.baseCase(2,3,2, nodeProps.doubleArrayValue(2))).isEqualTo(10); //corevalue

    }

    @Test
    void shouldIgnoreOnBasecaseForSameComponent(){
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
        var dualTreeMST =  DualTreeMSTAlgorithm.createWithZeroCores(nodeProps,kdTree,8);
        dualTreeMST.mergeComponents(0,1);

        assertThat(dualTreeMST.baseCase(0,1, 0, nodeProps.doubleArrayValue(0))).isEqualTo(-1.0);

    }

    @Test
    void shouldUpdateBoundsCorrectly(){
        var props = mock(NodePropertyValues.class);
        var kdTree = mock(KdTree.class);
        when(kdTree.treeNodeCount()).thenReturn(10L);
        var dualTreeMST =  DualTreeMSTAlgorithm.createWithZeroCores(props,kdTree,8);

        assertThat(dualTreeMST.updateBound(0,10)).isEqualTo(10);
        assertThat(dualTreeMST.updateBound(0,5)).isEqualTo(10);
        assertThat(dualTreeMST.updateBound(0,100)).isEqualTo(100);

    }

    @Test
    void traversalBetweenLeaves(){
        KdNode node1 = KdNode.createLeaf(0,0,2,new AABB(new double[]{0}, new double[]{4},1));
        KdNode node2 = KdNode.createLeaf(1,3,4,new AABB(new double[]{5}, new double[]{7},1));

        DoubleArrayNodePropertyValues nodeProps=new DoubleArrayNodePropertyValues() {
            private double[] provs=new double[]{0,4,7,5};
            @Override
            public double[] doubleArrayValue(long nodeId) {
                return new double[]{provs[(int)nodeId]};
            }

            @Override
            public long nodeCount() {
                return 0;
            }
        };
        //0,4 and  5,7

        // closet(0) = 5  , 5-0
        // closet(4) = 5  ,5-1

        //max {1,5} = 5
        var kdTree=new KdTree(HugeLongArray.of(0,1,2,3),nodeProps,null,2);

        var dualTree =  DualTreeMSTAlgorithm.createWithZeroCores(nodeProps,kdTree,4);

        dualTree.resetNodeBounds();
        dualTree.traversalLeafLeafStep(node1,node2);

        assertThat(dualTree.kdNodeBound(0)).isEqualTo(25); //doesnt do the root

    }


}
