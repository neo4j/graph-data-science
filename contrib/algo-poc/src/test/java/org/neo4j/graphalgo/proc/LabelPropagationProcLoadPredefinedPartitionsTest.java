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
package org.neo4j.graphalgo.proc;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.impl.utils.GetNodeFunc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class LabelPropagationProcLoadPredefinedPartitionsTest {

    private static final String DB_CYPHER = "" +
            "CREATE (a:A {id: 0, partition: 42})\n" +
            "CREATE (b:B {id: 1, partition: 42})\n" +
            "CREATE (c:C {id: 2, partition: 42})\n" +
            "CREATE (d:D {id: 3, partition: 29})\n" +
            "CREATE (e:E {id: 4, partition: 29})\n" +
            "CREATE (f:F {id: 5, partition: 29})\n" +
            "CREATE (g:G {id: 6, partition: 29}) ";

    @Parameterized.Parameters(name = "parallel={0}, graph={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, "heavy"},
                new Object[]{true, "heavy"},
                new Object[]{false, "huge"},
                new Object[]{true, "huge"}
        );
    }

    public static GraphDatabaseService DB;

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    private final boolean parallel;
    private final String graphImpl;

    public LabelPropagationProcLoadPredefinedPartitionsTest(boolean parallel, String graphImpl) {
        this.parallel = parallel;
        this.graphImpl = graphImpl;
    }

    @Before
    public void setup() throws KernelException {
        DB = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.procedure_unrestricted,"algo.*")
                .newGraphDatabase();

        Procedures proceduresService = ((GraphDatabaseAPI) DB).getDependencyResolver().resolveDependency(Procedures.class);

        proceduresService.registerProcedure(Procedures.class, true);
        proceduresService.registerProcedure(LabelPropagationProc.class, true);
        proceduresService.registerFunction(GetNodeFunc.class, true);

        DB.execute(DB_CYPHER);
    }

    @AfterClass
    public static void tearDown() {
        DB.shutdown();
    }

    @Test
    public void shouldUseDefaultValues() {
        String query = "CALL algo.labelPropagation.stream(null, null, {batchSize:$batchSize,concurrency:$concurrency,graph:$graph}) " +
                "YIELD nodeId, label " +
                "RETURN algo.asNode(nodeId) AS id, label " +
                "ORDER BY id";

        Result result = DB.execute(query, parParams());

        List<Integer> labels = result.columnAs("label").stream()
                .mapToInt(value -> ((Long)value).intValue()).boxed().collect(Collectors.toList());
        assertThat(labels, Matchers.is(Arrays.asList(42, 42, 42, 29, 29, 29,29)));
    }

    private Map<String, Object> parParams() {
        return MapUtil.map("batchSize", parallel ? 5 : 1, "concurrency", parallel ? 8 : 1, "graph", graphImpl);
    }
}
