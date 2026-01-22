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
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public abstract class AllShortestPathsStatsProcTest extends AllShortestPathsProcTest {

    @Inject
    IdFunction idFunction;

    @Test
    void testStats() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .statsMode()
            .addParameter("sourceNode", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", "cost")
            .yields("preProcessingMillis", "computeMillis", "postProcessingMillis", "configuration");

        //@formatter:off
        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
            assertCypherResult(query, List.of(Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "postProcessingMillis", 0L,
                "configuration", instanceOf(Map.class)
            )));
        });
        //@formatter:on
    }

    @Test
    void testMemoryEstimation() {
        var query = GdsCypher.call("graph")
            .algo(getProcedureName())
            .statsEstimation()
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
