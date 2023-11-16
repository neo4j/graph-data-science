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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

abstract class AllShortestPathsProcTest extends BaseTest
{
    private static final String GRAPH_NAME = "graph";
    // Track expected results
    long idA, idB, idC, idD, idE, idF;
    static double[] costs0, costs1, costs2, costs3, costs4, costs5;
    static long[] ids0, ids1, ids2, ids3, ids4, ids5;

    @Neo4jGraph
    public static final String DB_CYPHER = "CREATE" +
           "  (:Offset)" +
           ", (a:Label)" +
           ", (b:Label)" +
           ", (c:Label)" +
           ", (d:Label)" +
           ", (e:Label)" +
           ", (f:Label)" +
           ", (a)-[:TYPE {cost: 4}]->(b)" +
           ", (a)-[:TYPE {cost: 2}]->(c)" +
           ", (b)-[:TYPE {cost: 5}]->(c)" +
           ", (b)-[:TYPE {cost: 10}]->(d)" +
           ", (c)-[:TYPE {cost: 3}]->(e)" +
           ", (d)-[:TYPE {cost: 11}]->(f)" +
           ", (e)-[:TYPE {cost: 4}]->(d)";

    protected abstract Class<?> getProcedureClazz();

    protected abstract String getProcedureName();

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        GraphDatabaseApiProxy.registerProcedures(
            db,
            getProcedureClazz(),
            GraphProjectProc.class
        );

        idA = idFunction.of("a");
        idB = idFunction.of("b");
        idC = idFunction.of("c");
        idD = idFunction.of("d");
        idE = idFunction.of("e");
        idF = idFunction.of("f");

        costs0 = new double[]{0.0};
        costs1 = new double[]{0.0, 2.0};
        costs2 = new double[]{0.0, 4.0};
        costs3 = new double[]{0.0, 2.0, 5.0};
        costs4 = new double[]{0.0, 2.0, 5.0, 9.0};
        costs5 = new double[]{0.0, 2.0, 5.0, 9.0, 20.0};

        ids0 = new long[]{idA};
        ids1 = new long[]{idA, idC};
        ids2 = new long[]{idA, idB};
        ids3 = new long[]{idA, idC, idE};
        ids4 = new long[]{idA, idC, idE, idD};
        ids5 = new long[]{idA, idC, idE, idD, idF};

        runQuery(GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Label")
            .withAnyRelationshipType()
            .withRelationshipProperty("cost")
            .yields());
    }

    @AfterEach
    void teardown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

}
