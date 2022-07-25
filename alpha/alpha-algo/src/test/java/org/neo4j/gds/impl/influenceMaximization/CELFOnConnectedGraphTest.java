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

import static org.neo4j.gds.influenceMaximization.CELFAlgorithmFactory.DEFAULT_BATCH_SIZE;


@GdlExtension
class CELFOnConnectedGraphTest {
    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (a)-[:R]->(b) " +
        ", (a)-[:R]->(c) " +
        ", (a)-[:R]->(d) " +
        ", (d)-[:R]->(e) ";


    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testSpreadWithSeed1() {
        //monte-carlo 1
        // a->d (0.0264) : NOT a->b (0.8833) a->c (0.4315) d->e (0.8833)
        //monte-carlo 2
        // NOTHING ACTIVATED: a->b  (0.5665)  a->c  (0.7457) a->d (0.9710)  d->e (0.5665)

        //monte carlo 3
        // NOTHING ACTIVATED: a->b (0.5911) a->c (0.7491)  a->d(0.595) d-> (0.5911)

        //round 1         MC1                MC2                 MC3
        // gain[a] = (1 (a) + 1(d))      + ( 1(a) )     +     ( 1(a))
        //            = 2 + 1 + 1 = 4/3 = 1.33

        //round 2         MC1                               MC2                 MC3
        // gain[b|a] :       1(b)                           1(b)                1(b)  = 1
        // gain[c|a]:        1(c)                           1(c)                1(c)  = 1
        // gain[e|a]:        1(e)                           1(e)                1(e)  = 1
        //gain[d|a] :        0 {a already activates d}      1(d)                1(d) =  2/3 =0.667

        //choose {b,c,e} --> choose b

        //round 3
        // gain[c|a,b]:        1(c)                           1(c)                1(c)  = 1
        // gain[e|a,b]:        1(e)                           1(e)                1(e)  = 1
        // gain[d|a,b] :        0 {a already activates d}      1(d)                1(d)    =  2/3 =0.667

        //choose{c,e} -->choose c

        //round 4
        // gain[e|a,b,c]:        1(e)                           1(e)                1(e)  = 1
        // gain[d|a,b,d] :        0 {a already activates d}      1(d)                1(d)    =  2/3 =0.667

        //choose c

        //round 5
        // gain[d|a,b,d,e] :        0 {a already activates d}      1(d)                1(d)    =  2/3 =0.667


        CELF celf = new CELF(graph, 5, 0.2, 3, Pools.DEFAULT, 2, 0, DEFAULT_BATCH_SIZE);
        celf.compute();
        var softAssertions = new SoftAssertions();

        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("a")))
            .as("spread of a")
            .isEqualTo(4 / 3.0, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("b")))
            .as("spread of b")
            .isEqualTo(1, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("c")))
            .as("spread of c")
            .isEqualTo(1, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("d")))
            .as("spread of d")
            .isEqualTo(2 / 3.0, Offset.offset(1e-5));
        softAssertions
            .assertThat(celf.getNodeSpread(idFunction.of("e")))
            .as("spread of e")
            .isEqualTo(1, Offset.offset(1e-5));

        softAssertions.assertAll();
    }
}
