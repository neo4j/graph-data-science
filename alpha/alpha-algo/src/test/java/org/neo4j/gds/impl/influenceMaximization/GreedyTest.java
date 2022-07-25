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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.influenceMaximization.Greedy;

/**
 *     (c)-----|
 *    /(d)\----|-|
 *   //(e)\\---|-|-|
 *  ///(f)\\\--|-|-|-|
 * ////   \\\\ | | | |
 * (a)     (b) | | | |
 * \\\\   //// | | | |
 *  \\\(g)///--| | | |
 *   \\(h)//-----| | |
 *    \(i)/--------| |
 *     (j)-----------|
 */
@GdlExtension
final class GreedyTest {
    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (h:Node)" +
        ", (i:Node)" +
        ", (j:Node)" +

        ", (a)-[:RELATIONSHIP]->(c)" +
        ", (a)-[:RELATIONSHIP]->(d)" +
        ", (a)-[:RELATIONSHIP]->(e)" +
        ", (a)-[:RELATIONSHIP]->(f)" +
        ", (a)-[:RELATIONSHIP]->(g)" +
        ", (a)-[:RELATIONSHIP]->(h)" +
        ", (a)-[:RELATIONSHIP]->(i)" +
        ", (a)-[:RELATIONSHIP]->(j)" +

        ", (b)-[:RELATIONSHIP]->(c)" +
        ", (b)-[:RELATIONSHIP]->(d)" +
        ", (b)-[:RELATIONSHIP]->(e)" +
        ", (b)-[:RELATIONSHIP]->(f)" +
        ", (b)-[:RELATIONSHIP]->(g)" +
        ", (b)-[:RELATIONSHIP]->(h)" +
        ", (b)-[:RELATIONSHIP]->(i)" +
        ", (b)-[:RELATIONSHIP]->(j)" +

        ", (c)-[:RELATIONSHIP]->(g)" +
        ", (d)-[:RELATIONSHIP]->(h)" +
        ", (e)-[:RELATIONSHIP]->(i)" +
        ", (f)-[:RELATIONSHIP]->(j)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testSpread() {
        var greedy = new Greedy(graph, 10, 0.2, 10, Pools.DEFAULT, 2);
        greedy.compute();
        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("a"))).isEqualTo(2.6);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("b"))).isEqualTo(1.0);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("c"))).isEqualTo(0.9000000000000004);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("d"))).isEqualTo(0.9999999999999996);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("e"))).isEqualTo(0.7999999999999998);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("f"))).isEqualTo(0.6999999999999993);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("g"))).isEqualTo(0.5999999999999996);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("h"))).isEqualTo(0.7999999999999998);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("i"))).isEqualTo(0.7000000000000011);
        softAssertions.assertThat(greedy.getNodeSpread(idFunction.of("j"))).isEqualTo(0.9000000000000004);

        softAssertions.assertAll();
    }
}
