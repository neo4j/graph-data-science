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
package org.neo4j.gds.paths.singlesource;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.paths.PathFactory;
import org.neo4j.graphdb.Path;

import java.util.List;
import java.util.Map;

import static org.assertj.core.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

public abstract class AllShortestPathsStreamProcTest extends AllShortestPathsProcTest {
    @Test
    void testStream() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .streamMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", "cost")
            .yields("sourceNode", "targetNode", "totalCost", "costs", "nodeIds", "path") +
            " RETURN  sourceNode, targetNode, totalCost, costs, nodeIds, path" +
            " ORDER BY totalCost";

        //@formatter:off
        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
            var expected = List.of(
                Map.of("sourceNode", idA, "targetNode", idA, "totalCost", 0.0D, "costs", asList(costs0), "nodeIds", asList(ids0), "path", isA(Path.class)),
                Map.of("sourceNode", idA, "targetNode", idC, "totalCost", 2.0D, "costs", asList(costs1), "nodeIds", asList(ids1), "path", isA(Path.class)),
                Map.of("sourceNode", idA, "targetNode", idB, "totalCost", 4.0D, "costs", asList(costs2), "nodeIds", asList(ids2), "path", isA(Path.class)),
                Map.of("sourceNode", idA, "targetNode", idE, "totalCost", 5.0D, "costs", asList(costs3), "nodeIds", asList(ids3), "path", isA(Path.class)),
                Map.of("sourceNode", idA, "targetNode", idD, "totalCost", 9.0D, "costs", asList(costs4), "nodeIds", asList(ids4), "path", isA(Path.class)),
                Map.of("sourceNode", idA, "targetNode", idF, "totalCost", 20.0D, "costs", asList(costs5), "nodeIds", asList(ids5), "path", isA(Path.class))
            );
            PathFactory.RelationshipIds.set(0);
            assertCypherResult(query, expected);
        });
        //@formatter:on

    }

    @Test
    void testMemoryEstimation() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .estimationMode(GdsCypher.ExecutionModes.STREAM)
            .addParameter("sourceNode", idFunction.of("a"))
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        assertCypherResult(query, List.of(Map.of(
            "bytesMin", greaterThan(0L),
            "bytesMax", greaterThan(0L),
            "nodeCount", 6L,
            "relationshipCount", 7L
        )));
    }
}
