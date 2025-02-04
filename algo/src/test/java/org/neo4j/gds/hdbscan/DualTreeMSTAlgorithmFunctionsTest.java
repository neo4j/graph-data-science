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

import org.apache.commons.lang3.mutable.MutableDouble;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

import static org.assertj.core.api.Assertions.assertThat;
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

        var dualTreeMST = new DualTreeMSTAlgorithm(null,kdTree,null,4);

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
        var dualTreeMST = new DualTreeMSTAlgorithm(null,kdTree,null,4);
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

        var dualTreeMST = new DualTreeMSTAlgorithm(null,kdTree,null,8);

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
        var dualTreeMST = new DualTreeMSTAlgorithm(nodeProps,kdTree, HugeDoubleArray.of(0,0,10,10),8);


        var maxBound01 =new MutableDouble(-100.0);
        dualTreeMST.baseCase(0,1,maxBound01);
        assertThat(maxBound01.doubleValue()).isEqualTo(1); //distance

        var maxBound23 =new MutableDouble(-100.0);
        dualTreeMST.baseCase(2,3,maxBound23);
        assertThat(maxBound23.doubleValue()).isEqualTo(10); //corevalue


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
        var dualTreeMST = new DualTreeMSTAlgorithm(nodeProps,kdTree,null,8);
        dualTreeMST.mergeComponents(0,1);

        var maxBound =new MutableDouble(-100.0);
        dualTreeMST.baseCase(0,1,maxBound);

        assertThat(maxBound.doubleValue()).isEqualTo(-100.0);

    }

    @Test
    void shouldUpdateBoundsCorrectly(){
        var props = mock(NodePropertyValues.class);
        var kdTree = mock(KdTree.class);
        when(kdTree.treeNodeCount()).thenReturn(10L);
        var dualTreeMST = new DualTreeMSTAlgorithm(props,kdTree,null,8);

        assertThat(dualTreeMST.updateBound(0,10)).isEqualTo(10);
        assertThat(dualTreeMST.updateBound(0,5)).isEqualTo(10);
        assertThat(dualTreeMST.updateBound(0,100)).isEqualTo(100);

    }

  
}
