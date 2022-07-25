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
package org.neo4j.gds.impl.influenceMaximization;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.influenceMaximization.CELF;


@GdlExtension
class CELFTestOnTreeGraph {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a1:Node)" +
        ", (a2:Node)" +
        ", (a3:Node)" +
        ", (a4:Node)" +
        ", (a5:Node)" +
        ", (a1)-[:R]->(a2) " +
        ", (a1)-[:R]->(a3) " +
        ", (a2)-[:R]->(a3) " +
        ", (a2)-[:R]->(a4) " +
        ", (a2)-[:R]->(a5) " +
        ", (a3)-[:R]->(a5) " +
        ", (a5)-[:R]->(a4) ";

    //d(a1) =2
    //d(a2) =4
    //d(a3)= 3
    //d(a4)=2
    //d(a5)=3
    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testSpreadWithSeed1() {

        //a1 : a2 a3
        //a2: a1 a3 a4 a5
        //a3: a1 a2 a5
        //a4: a2 a5
        //a5: a2 a3 a4
        //      0st neighbor(0,1)              1nd (1)                   2rd (0,2)                  3th(1)
        //MC1: 0.033311053770689214     0.7343671530089991      0.13097277147873954         0.8417750378811617  | (0,2)
        //MC2: 0.3162443929209082       0.2623651517737182      0.6380423420183485          0.5046140312107866  | (0,1,3)
        //MC3: 0.579101204080752        0.939463266780566       0.2347388172488516          0.904839039446324   | (2)

        // MC1 : a1 -> a2                   MC2     :  a1 -> a2 a3              MC3:  a1 -> x
        //       a2 -> a1 a4                           a2 -> a1 a3 a5                 a2-> a4
        //       a3 -> a1 a5                           a3 -> a1 a2                    a3-> a5
        //       a4 -> a2                              a4 -> a2 a5                    a4-> x
        //       a5 -> a2 a4                           a5->  a2 a3                    a5-> a4


        //MC1:  a1-> a1,a2,a4        (3)             MC2:    a1->a1,a2,a3,a5      (4)          MC3:    a1->a1        (1)
        //      a2 ->a1,a2,a4        (3)                     a2-> a2,a2,a3,a5     (4)                  a2->a2,a4     (2)
        //      a3 -> a1,a2,a3,a4,a5 (5)                     a3-> a1,a2,a3,a5     (4)                  a3-> a3,a4,a5 (3)
        //      a4 -> a1,a2,a4       (3)                     a4-> a1 a2 a3 a4 a5  (5)                  a4->a4        (1)
        //      a5-> a1,a2,a4,a5     (4)                    a5-> a1,a2,a3,a5      (4)                  a5->a4,a5     (2)


        //round-1
        //gain[a1] =   (3 + 4 + 1)/3    = 8/3
        //gain[a2] =   (3 +4 + 2) /3    = 9/3
        //gain[a3] =   (5+4+3)/3        =12/3 x
        //gain[a4]=    (3+5+1)/3        =9/3
        //gain[a5]= (4+4+2)/3=          =10/3


        //MC1:
        //
        // a3 -> a1,a2,a3,a4,a5 (5)                     a3-> a1,a2,a3,a5     (4)                  a3-> a3,a4,a5 (3)

        // a1-> (0)             MC2:   a1->       (0)          MC3:    a1->a1        (1)
        //a2 -> (0)                    a2->       (0)                  a2->a2        (1)
        //a4 -> (0)                    a4-> a4    (1)                  a4->a4        (0)
        //a5->  (0)                    a5-> 0     (0)                  a5->a4,a5     (0)


        //round-2
        //gain[a1 | a3] =   1/3
        //gain[a2| a3] =    1/3
        //gain[a4 | a3]=    1/3
        //gain[a5 | a3] =   0
        //chooese (a1,a2,a4)  --> a1


        //round-3

        // a1,a3 -> a1,a2,a3,a4,a5 (5)                     a1,a3-> a1,a2,a3,a5     (4)                  a1,a3-> a1,a3,a4,a5 (3)

        //a2 -> (0)                    a2->       (0)                  a2->a2        (1)
        //a4 -> (0)                    a4-> a4    (1)                  a4->a4        (0)
        //a5->  (0)                    a5-> 0     (0)                  a5->a4,a5     (0)

        //chooses (a2,a4)   --> a2


        //then  a4 is picked with 1/3 (independntant)

        //finally a5 has a gain of 0
        CELF celf = new CELF(graph, 5, 0.51, 3, Pools.DEFAULT, 1, 10);
        celf.compute();
        var softAssertions = new SoftAssertions();

        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("a1")))
            .as("spread of a1")
            .isEqualTo(1 / 3.0, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("a2")))
            .as("spread of a2")
            .isEqualTo(1 / 3.0, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("a3")))
            .as("spread of a3")
            .isEqualTo(4.0, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("a4")))
            .as("spread of a4")
            .isEqualTo(1 / 3.0, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("a5")))
            .as("spread of a5")
            .isEqualTo(0, Offset.offset(1e-5));

        softAssertions.assertAll();
    }
}
