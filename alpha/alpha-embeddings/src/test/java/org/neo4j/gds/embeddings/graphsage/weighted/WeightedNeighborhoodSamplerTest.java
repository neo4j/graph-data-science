/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.weighted;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class WeightedNeighborhoodSamplerTest {

    @GdlGraph
    private static final String DB_QUERY =
        "CREATE " +
        "   (u1:User), (u1:User)," +
        "   (d1:Dish),(d2:Dish),(d3:Dish),(d4:Dish)," +
        "   (u1)-[:ORDERED {times: 5}]->(d1), " +
        "   (u1)-[:ORDERED {times: 2}]->(d2), " +
        "   (u1)-[:ORDERED {times: 1}]->(d3), " +
        "   (u2)-[:ORDERED {times: 2}]->(d3), " +
        "   (u2)-[:ORDERED {times: 3}]->(d4)";

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testSampling() {
        var sampler = new WeightedNeighborhoodSampler();

        var u1Neighbors = sampler.sample(graph, idFunction.of("u1"), 2);
        assertThat(u1Neighbors).containsExactlyInAnyOrder(idFunction.of("d1"), idFunction.of("d2"));

        var u2Neighbors = sampler.sample(graph, idFunction.of("u2"), 1);
        assertThat(u2Neighbors).containsExactly(idFunction.of("d4"));
    }
}
