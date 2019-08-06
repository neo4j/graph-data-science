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

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.hamcrest.MatcherAssert.assertThat;

public class LabelPropagationProcLoadPredefinedLabelsTest extends ProcTestBase {

    private static final String DB_CYPHER = "CREATE " +
            "  (a:A {id: 0, community: 42})" +
            ", (b:B {id: 1, community: 42})" +
            ", (c:C {id: 2, community: 42})" +
            ", (d:D {id: 3, community: 29})" +
            ", (e:E {id: 4, community: 29})" +
            ", (f:F {id: 5, community: 29})" +
            ", (g:G {id: 6, community: 29})";

    static Stream<Arguments> parameters() {
        return Stream.of(
                arguments(false, "heavy"),
                arguments(true,  "heavy"),
                arguments(false, "huge"),
                arguments(true,  "huge")
        );
    }

    public static GraphDatabaseService DB;

    @BeforeEach
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

    @AfterAll
    public static void tearDown() {
        DB.shutdown();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void shouldUseDefaultValues(boolean parallel, String graphImpl) {
        String query = "CALL algo.labelPropagation.stream(" +
                       "    null, null, {" +
                       "        batchSize: $batchSize, concurrency: $concurrency, graph: $graph, seedProperty: 'community'" +
                       "    }" +
                       ") YIELD nodeId, community " +
                       "RETURN algo.asNode(nodeId) AS id, community " +
                       "ORDER BY id";

        Result result = DB.execute(query, parParams(parallel, graphImpl));

        List<Integer> labels = result.columnAs("community").stream()
                .mapToInt(value -> ((Long)value).intValue()).boxed().collect(Collectors.toList());
        assertThat(labels, Matchers.is(Arrays.asList(42, 42, 42, 29, 29, 29,29)));
    }

    private Map<String, Object> parParams(boolean parallel, String graphImpl) {
        return MapUtil.map("batchSize", parallel ? 5 : 1, "concurrency", parallel ? 8 : 1, "graph", graphImpl);
    }
}
