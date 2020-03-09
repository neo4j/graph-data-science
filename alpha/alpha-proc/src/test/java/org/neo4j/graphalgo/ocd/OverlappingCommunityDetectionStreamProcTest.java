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
package org.neo4j.graphalgo.ocd;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import java.util.HashSet;
import java.util.Set;

public class OverlappingCommunityDetectionStreamProcTest extends BaseProcTest {
    /**
     * (a)-- (b)--(d)--(e)
     * \T1/       \T2/
     * (c)   (g)  (f)
     * \  /T3\
     * (h)--(i)
     */
    String dbCypher() {
        return
            "CREATE " +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(a)" +
            ", (c)-[:TYPE]->(h)" +
            ", (d)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(d)" +
            ", (b)-[:TYPE]->(d)" +
            ", (g)-[:TYPE]->(h)" +
            ", (h)-[:TYPE]->(i)" +
            ", (i)-[:TYPE]->(g)";
    }

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase((builder) ->
            builder.setConfig(GraphDatabaseSettings.procedure_unrestricted, "gds.util.*")
        );
        registerProcedures(OverlappingCommunityDetectionStreamProc.class);
        registerFunctions(GetNodeFunc.class);
        runQuery(dbCypher());
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    @Test
    void stream() {
        String q = "CALL gds.alpha.ocd.stream({nodeProjection: \"*\", relationshipProjection: {`*`: {type: \"*\", orientation: \"UNDIRECTED\"}}, concurrency: 1})" +
                   " YIELD nodeId, communityIds, scores" +
                   " RETURN nodeId, gds.util.asNode(nodeId).name as node, communityIds, scores";

        Set<String> actualRowDescriptions = new HashSet<>();

/*
        runQueryWithRowConsumer(q, row -> {
            System.out.println(row);
        });
*/

        runQueryWithResultConsumer(q, result -> System.out.println(result.resultAsString()));
    }
}
