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
package org.neo4j.graphalgo.shortestpaths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphdb.Path;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class YensKShortestPathsStreamProcTest extends BaseProcTest {

    @BeforeEach
    void setupGraph() throws Exception {
        final String cypher =
                "CREATE" +
                "  (a)" + // 0
                ", (b)" + // 1
                ", (c)" + // 2
                ", (d)" + // 3
                ", (e:End)" + // 4
                ", (f:Start)" + // 5
                ", (g)" + // 6
                ", (h)" + // 7
                ", (i)" + // 8
                ", (j)" + // 9
                ", (k)" + // 10
                ", (l)" + // 11
                ", (m)" + // 12
                ", (n)" + // 13
                ", (o)" + // 14
                ", (p)" + // 15
                ", (q)" + // 16
                ", (r)" + // 17
                ", (s)" + // 18
                ", (t)" + // 19
                ", (a)-[:REL]->(b)" +
                ", (b)-[:REL]->(i)" +
                ", (c)-[:REL]->(b)" +
                ", (c)-[:REL]->(d)" +
                ", (d)-[:REL]->(k)" +
                ", (d)-[:REL]->(l)" +
                ", (d)-[:REL]->(e)" +
                ", (e)-[:REL]->(n)" +
                ", (f)-[:REL]->(a)" +
                ", (g)-[:REL]->(f)" +
                ", (g)-[:REL]->(h)" +
                ", (i)-[:REL]->(b)" +
                ", (i)-[:REL]->(g)" +
                ", (k)-[:REL]->(i)" +
                ", (k)-[:REL]->(j)" +
                ", (m)-[:REL]->(l)" +
                ", (p)-[:REL]->(a)" +
                ", (p)-[:REL]->(k)" +
                ", (p)-[:REL]->(l)" +
                ", (q)-[:REL]->(i)" +
                ", (q)-[:REL]->(r)" +
                ", (r)-[:REL]->(m)" +
                ", (r)-[:REL]->(n)" +
                ", (s)-[:REL]->(q)" +
                ", (s)-[:REL]->(n)" +
                ", (t)-[:REL]->(s)";

        runQuery(cypher);
        registerProcedures(KShortestPathsProc.class);
    }

    @Test
    void test() {
        String algoCall = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds.alpha.kShortestPaths")
            .streamMode()
            .addVariable("startNode", "from")
            .addVariable("endNode", "to")
            .addParameter("k", 3)
            .addParameter("path", true)
            .yields("path");

        String cypher = formatWithLocale(
            "MATCH (from:Start), (to:End) %s RETURN length(path) AS length, path ORDER BY length ASC",
            algoCall
        );

        try (GraphDatabaseApiProxy.Transactions transactions = newKernelTransaction(db)) {
            List<Path> expected = List.of(
                WalkPath.toPath(transactions.ktx(), new long[]{5, 0, 1, 2, 3, 4}),
                WalkPath.toPath(transactions.ktx(), new long[]{5, 0, 15, 10, 3, 4}),
                WalkPath.toPath(transactions.ktx(), new long[]{5, 6, 8, 10, 3, 4})
            );
            List<Path> actual = new ArrayList<>();
            runQueryWithRowConsumer(cypher, row -> actual.add(row.getPath("path")));

            // Need to String equality here, since Path does not override equals / hashCode
            assertEquals(expected.toString(), actual.toString());
        }
    }
}
