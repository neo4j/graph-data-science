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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TreeProducerTest {

    @Test
    void shouldCreateACorrectTree(){


        var treeEdges = HugeLongArray.of(0,1,2,3,4);
        var numberOfTreeEdges= 5;

        BitSet bitSet=new BitSet(6);
        bitSet.set(0);
        bitSet.set(1);
        bitSet.set(2);
        bitSet.set(3);

        var edgeParts= HugeLongArray.of(0,1,0,2,5,5,1,2,2,3);
        var edgeCosts = HugeDoubleArray.of(1.0,2.0,3.0,4.0,5.0);


        var growthResult = new GrowthResult(treeEdges,numberOfTreeEdges,edgeParts,edgeCosts,bitSet);

        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(5)
            .concurrency(new Concurrency(1))
            .build();

        nodesBuilder.addNode(0);
        nodesBuilder.addNode(1);
        nodesBuilder.addNode(2);
        nodesBuilder.addNode(3);
        nodesBuilder.addNode(4);
        nodesBuilder.addNode(5);


        var idMap = nodesBuilder.build().idMap();

        var treeStructure = TreeProducer.createTree(growthResult,6, idMap, ProgressTracker.NULL_TRACKER);

        var degrees=treeStructure.degrees();
        var tree = treeStructure.tree();

        assertThat(degrees.toArray()).containsExactly(2,2,3,1,0,0);

        long[][] expectedNeighbors=new long[4][];
        expectedNeighbors[0]=new long[]{1,2};
        expectedNeighbors[1]=new long[]{0,2};
        expectedNeighbors[2]=new long[]{0,1,3};
        expectedNeighbors[3]=new long[]{2};

        double[][] expectedWeights=new double[4][];
        expectedWeights[0]=new double[]{1.0,2.0};
        expectedWeights[1]=new double[]{1.0,4.0};
        expectedWeights[2]=new double[]{1.0,4.0,5.0};
        expectedWeights[3]=new double[]{5.0};

        for (int u=0;u<4;++u){
            Set<Long> neighbors=new HashSet<>();
            Set<Double> weights =new HashSet<>();
            tree.forEachRelationship(u, -1.0, (s,t,w)->{
                neighbors.add(t);
                weights.add(w);
                return true;
            });
            for (int j=0;j < expectedNeighbors[u].length;++j){
                assertThat(neighbors.contains(expectedNeighbors[u][j]));
                assertThat(weights.contains(expectedWeights[u][j]));
            }
        }
        assertThat(tree.degree(4)).isEqualTo(0L);
        assertThat(tree.degree(5)).isEqualTo(0L);

    }

}
