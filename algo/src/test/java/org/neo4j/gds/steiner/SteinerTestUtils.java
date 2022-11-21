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
package org.neo4j.gds.steiner;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.data.Offset;
import org.neo4j.gds.extension.IdFunction;

import static org.assertj.core.api.Assertions.assertThat;

public class SteinerTestUtils {
    final static Offset  offset= Offset.offset(1e-5);

     static void assertTreeIsCorrect(IdFunction idFunction,SteinerTreeResult steinerTreeResult, long[] expectedParent, double[] expectedParentCost, double expectedCost){
         var parent=steinerTreeResult.parentArray();
         var parentCost=steinerTreeResult.relationshipToParentCost();
         var cost=steinerTreeResult.totalCost();
         SoftAssertions softAssertions=new SoftAssertions();
         assertThat(cost).isCloseTo(expectedCost,offset);
        long nodeCount=parent.size();
        for (int indexId=0;indexId<nodeCount;++indexId){
            long nodeId=idFunction.of("a"+indexId);
            softAssertions.assertThat(parent.get(nodeId)).isEqualTo(expectedParent[indexId]);
            softAssertions.assertThat(parentCost.get(nodeId)).isCloseTo(expectedParentCost[indexId],offset);
        }
        softAssertions.assertAll();
    }

    static long[] getNodes(IdFunction idFunction,int n){
            long[] nodes=new long[n];
            for (int i=0;i<n;++i){
                nodes[i]=idFunction.of("a"+i);
            }
            return  nodes;
    }
}
