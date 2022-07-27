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
package org.neo4j.gds.beta.walking;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class CollapseMultiPathsTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DATA =
        "CREATE" +
        "  (baltimore:City          {id:  0})" +
        "  (chicago:City            {id:  1})" +
        "  (hannibal:City           {id:  2})" +
        "  (jeffersoncity:City      {id:  3})" +
        "  (junctioncity:City       {id:  4})" +
        "  (jfk:Airport             {id:  5})" +
        "  (kansas:City             {id:  6})" +
        "  (lasallevanburen:Station {id:  7})" +
        "  (losangeles:City         {id:  8})" +
        "  (manhattan               {id:  9})" +
        "  (nashville:City          {id: 10})" +
        "  (newyork:City            {id: 11})" +
        "  (philadelphia:City       {id: 12})" +
        "  (statenisland:Island     {id: 13})" +
        "  (stlouis:City            {id: 14})" +
        "  (topeka:City             {id: 15})" +
        "  (washington:City         {id: 16})" +
        "  (wichita:City            {id: 17})" +

        ", (baltimore)-[:CAR]->(philadelphia)" +
        ", (baltimore)-[:TRAIN]->(philadelphia)" +
        ", (hannibal)-[:TRAIN]->(chicago)" +
        ", (jeffersoncity)-[:BUS]->(stlouis)" +
        ", (junctioncity)-[:PLANE]->(topeka)" +
        ", (jfk)-[:PLANE]->(wichita)" +
        ", (kansas)-[:TRAIN]->(hannibal)" +
        ", (losangeles)-[:TRAIN]->(junctioncity)" +
        ", (losangeles)-[:PLANE]->(chicago)" +
        ", (manhattan)-[:FERRY]->(statenisland)" +
        ", (manhattan)-[:TAXI]->(jfk)" +
        ", (philadelphia)-[:CAR]->(newyork)" +
        ", (philadelphia)-[:TRAIN]->(newyork)" +
        ", (stlouis)-[:CAR]->(lasallevanburen)" +
        ", (topeka)-[:CAR]->(kansas)" +
        ", (washington)-[:CAR]->(baltimore)" +
        ", (washington)-[:PLANE]->(newyork)" +
        ", (washington)-[:TRAIN]->(baltimore)" +
        ", (wichita)-[:TRAIN]->(jeffersoncity)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldFollowRoutesFromMovies() {
        var bus = graphStore.getGraph(RelationshipType.of("BUS"));
        var car = graphStore.getGraph(RelationshipType.of("CAR"));
        var ferry = graphStore.getGraph(RelationshipType.of("FERRY"));
        var plane = graphStore.getGraph(RelationshipType.of("PLANE"));
        var taxi = graphStore.getGraph(RelationshipType.of("TAXI"));
        var train = graphStore.getGraph(RelationshipType.of("TRAIN"));

        List<Graph[]> pathTemplates = List.of(
            new Graph[]{taxi, plane, train, bus, car},
            new Graph[]{ferry},
            new Graph[]{plane},
            new Graph[]{train, plane, car, train, train}
        );

        Relationships path = new CollapsePath(
            pathTemplates,
            false,
            2,
            Pools.DEFAULT

        ).compute();

        // Planes, Trains and Automobiles
        long manhattan = idFunction.of("manhattan");
        long lasallevanburen = idFunction.of("lasallevanburen");

        // King of Staten Island
        long statenisland = idFunction.of("statenisland");

        // Silver Streak + Airplane!
        long losangeles = idFunction.of("losangeles");
        long chicago = idFunction.of("chicago");

        try (AdjacencyList adjacencyList = path.topology().adjacencyList()) {
            AdjacencyCursor adjacencyCursor = adjacencyList.adjacencyCursor(manhattan);

            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromManhattan1 = adjacencyCursor.nextVLong();
            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromManhattan2 = adjacencyCursor.nextVLong();
            assertFalse(adjacencyCursor.hasNextVLong());

            assertThat(Set.of(lasallevanburen, statenisland))
                .containsExactlyInAnyOrder(
                    targetOfCollapsedPathFromManhattan1,
                    targetOfCollapsedPathFromManhattan2
                );

            // notice how this is a collapsed relationship per route
            // Silver Streak route and Airplane! route
            adjacencyCursor = adjacencyList.adjacencyCursor(losangeles);

            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromLosAngeles1 = adjacencyCursor.nextVLong();
            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromLosAngeles2 = adjacencyCursor.nextVLong();
            assertFalse(adjacencyCursor.hasNextVLong());

            assertThat(List.of(chicago, chicago))
                .containsExactlyInAnyOrder(
                    targetOfCollapsedPathFromLosAngeles1,
                    targetOfCollapsedPathFromLosAngeles2
                );
        }
    }

    @Test
    void shouldTurnEveryRouteIntoRelationship() {
        var car = graphStore.getGraph(RelationshipType.of("CAR"));
        var plane = graphStore.getGraph(RelationshipType.of("PLANE"));
        var train = graphStore.getGraph(RelationshipType.of("TRAIN"));

        Relationships path = new CollapsePath(
            List.of(
                new Graph[]{car, car, car},
                new Graph[]{train, train, train},
                new Graph[]{plane}
            ),
            false,
            2,
            Pools.DEFAULT

        ).compute();

        // US north east corridor
        long washington = idFunction.of("washington");
        long newyork = idFunction.of("newyork");

        try (AdjacencyList adjacencyList = path.topology().adjacencyList()) {
            AdjacencyCursor adjacencyCursor = adjacencyList.adjacencyCursor(washington);

            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromWashington1 = adjacencyCursor.nextVLong();
            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromWashington2 = adjacencyCursor.nextVLong();
            assertTrue(adjacencyCursor.hasNextVLong());
            long targetOfCollapsedPathFromWashington3 = adjacencyCursor.nextVLong();
            assertFalse(adjacencyCursor.hasNextVLong());

            // train, plane, automobile
            assertThat(newyork).isEqualTo(targetOfCollapsedPathFromWashington1);
            assertThat(newyork).isEqualTo(targetOfCollapsedPathFromWashington2);
            assertThat(newyork).isEqualTo(targetOfCollapsedPathFromWashington3);
        }
    }
}
