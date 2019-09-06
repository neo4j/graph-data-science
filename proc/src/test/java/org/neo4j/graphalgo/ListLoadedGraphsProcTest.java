/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListLoadedGraphsProcTest extends ProcTestBase {

    private static final String LOAD_QUERY = "CALL algo.graph.list() \n" +
                                             "YIELD name, nodes, relationships, type, direction";

    private static final String DB_CYPHER = "CREATE ()-[:TEST]->()";

    GraphDatabaseAPI db;

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LoadGraphProc.class);

        db.execute(DB_CYPHER);
    }

    @AfterEach
    public void tearDown() {
        LoadGraphFactory.remove("foo1");
        LoadGraphFactory.remove("foo2");
    }

    @Test
    public void shouldReturnEmptyList() {
        runQuery(LOAD_QUERY, db, Assertions::assertNull);
    }

    @Test
    public void shouldListAllAvailableGraphs() {
        String loadQuery = "CALL algo.graph.load(" +
                           "    $name, null, null, {" +
                           "        graph: $type, direction: $direction" +
                           "    }" +
                           ")";

        HashMap<String, Object> parameters1 = new HashMap<>();
        HashMap<String, Object> parameters2 = new HashMap<>();

        parameters1.put("name", "foo1");
        parameters1.put("type", "huge");
        parameters1.put("direction", "OUTGOING");
        parameters1.put("nodes", 2L);
        parameters1.put("relationships", 1L);

        runQuery(loadQuery, db, parameters1);

        parameters2.put("name", "foo2");
        parameters2.put("type", "kernel");
        parameters2.put("direction", "OUTGOING");
        parameters2.put("nodes", 2L);
        parameters2.put("relationships", 1L);

        runQuery(loadQuery, db, parameters2);

        List<Map<String, Object>> actual = new ArrayList<>();

        runQuery(LOAD_QUERY, db, resultRow -> {
            HashMap<String, Object> row = new HashMap<>();
            row.put("name", resultRow.getString("name"));
            row.put("type", resultRow.getString("type"));
            row.put("relationships", resultRow.getNumber("relationships"));
            row.put("nodes", resultRow.getNumber("nodes"));
            row.put("direction", resultRow.getString("direction"));

            actual.add(row);
        });

        assertEquals(parameters1, actual.get(0));
        assertEquals(parameters2, actual.get(1));
    }
}
