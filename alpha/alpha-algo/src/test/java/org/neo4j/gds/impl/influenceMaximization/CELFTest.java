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

@GdlExtension
final class CELFTest {
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
    void testSpreadFirstGraph() {
        CELF celf = new CELF(graph, 10, 0.2, 10, Pools.DEFAULT, 2);
        celf.compute();
        var softAssertions = new SoftAssertions();

        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("a"))).isEqualTo(2.2);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("b"))).isEqualTo(4.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("c"))).isEqualTo(5.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("d"))).isEqualTo(6.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("e"))).isEqualTo(7.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("f"))).isEqualTo(8.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("g"))).isEqualTo(9.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("h"))).isEqualTo(10.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("i"))).isEqualTo(11.4);
        softAssertions.assertThat(celf.getNodeSpread(idFunction.of("j"))).isEqualTo(12.4);

        softAssertions.assertAll();
    }
}
