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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class LabelPropagationProcTest {

    private static final String DB_CYPHER = "" +
            "CREATE (a:A {id: 0, partition: 42}) " +
            "CREATE (b:B {id: 1, partition: 42}) " +

            "CREATE (a)-[:X]->(:A {id: 2, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 3, weight: 2.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 4, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 5, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 6, weight: 8.0, partition: 2}) " +

            "CREATE (b)-[:X]->(:B {id: 7, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 8, weight: 2.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 9, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 10, weight: 1.0, partition: 1})" +
            "CREATE (b)-[:X]->(:B {id: 11, weight: 8.0, partition: 2})";

    @Parameterized.Parameters(name = "parallel={0}, graph={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{false, "heavy"},
                new Object[]{true, "heavy"},
                new Object[]{false, "huge"},
                new Object[]{true, "huge"}
        );
    }

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    private final boolean parallel;
    private final String graphImpl;

    public LabelPropagationProcTest(boolean parallel, String graphImpl) {
        this.parallel = parallel;
        this.graphImpl = graphImpl;
    }

    @Before
    public void setup() throws KernelException {
        db.resolveDependency(Procedures.class).registerProcedure(LabelPropagationProc.class);
        db.execute(DB_CYPHER);
    }

    @Test
    public void shouldUseDefaultValues() {
        String query = "CALL algo.labelPropagation()";

        runQuery(query, row -> {
            assertEquals(1, row.getNumber("iterations").intValue());
            assertEquals("weight", row.getString("weightProperty"));
            assertEquals("partition", row.getString("partitionProperty"));
            assertEquals("partition", row.getString("writeProperty"));
            assertTrue(row.getBoolean("write"));
        });
    }

    @Test
    public void explicitWriteProperty() {
        String query = "CALL algo.labelPropagation(null, null, null, {writeProperty: 'lpa'})";

        runQuery(query, row -> {
            assertEquals(1, row.getNumber("iterations").intValue());
            assertEquals("weight", row.getString("weightProperty"));
            assertEquals("partition", row.getString("partitionProperty"));
            assertEquals("lpa", row.getString("writeProperty"));
            assertTrue(row.getBoolean("write"));
        });
    }

    @Test
    public void shouldTakeParametersFromConfig() {
        String query = "CALL algo.labelPropagation(null, null, null, {iterations:5,write:false,weightProperty:'score',partitionProperty:'key'})";

        runQuery(query, row -> {
            assertTrue(5 >= row.getNumber("iterations").intValue());
            assertTrue(row.getBoolean("didConverge"));
            assertFalse(row.getBoolean("write"));
            assertEquals("score", row.getString("weightProperty"));
            assertEquals("key", row.getString("partitionProperty"));
        });
    }

    @Test
    public void shouldRunLabelPropagation() {
        String query = "CALL algo.labelPropagation(null, 'X', 'OUTGOING', {batchSize:$batchSize,concurrency:$concurrency,graph:$graph})";
        String check = "MATCH (n) WHERE n.id IN [0,1] RETURN n.partition AS partition";

        runQuery(query, parParams(), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertTrue(row.getBoolean("write"));

            assertTrue(
                    "load time not set",
                    row.getNumber("loadMillis").intValue() >= 0);
            assertTrue(
                    "compute time not set",
                    row.getNumber("computeMillis").intValue() >= 0);
            assertTrue(
                    "write time not set",
                    row.getNumber("writeMillis").intValue() >= 0);
        });
        runQuery(check, row ->
                assertEquals(2, row.getNumber("partition").intValue()));
    }

    @Test
    public void shouldFallbackToNodeIdsForNonExistingPartitionKey() {
        String query = "CALL algo.labelPropagation(null, 'X', 'OUTGOING', {partitionProperty:'foobar',batchSize:$batchSize,concurrency:$concurrency,graph:$graph})";
        String checkA = "MATCH (n) WHERE n.id = 0 RETURN n.foobar as partition";
        String checkB = "MATCH (n) WHERE n.id = 1 RETURN n.foobar as partition";

        runQuery(query, parParams(), row ->
                assertEquals("foobar", row.getString("partitionProperty")));
        runQuery(checkA, row ->
                assertEquals(6, row.getNumber("partition").intValue()));
        runQuery(checkB, row ->
                assertEquals(11, row.getNumber("partition").intValue()));
    }

    @Test
    public void shouldFilterByLabel() {
        String query = "CALL algo.labelPropagation('A', 'X', 'OUTGOING', {batchSize:$batchSize,concurrency:$concurrency,graph:$graph})";
        String checkA = "MATCH (n) WHERE n.id = 0 RETURN n.partition as partition";
        String checkB = "MATCH (n) WHERE n.id = 1 RETURN n.partition as partition";

        runQuery(query, parParams());
        runQuery(checkA, row ->
                assertEquals(2, row.getNumber("partition").intValue()));
        runQuery(checkB, row ->
                assertEquals(42, row.getNumber("partition").intValue()));
    }

    @Test
    public void shouldPropagateIncoming() {
        String query = "CALL algo.labelPropagation('A', 'X', 'INCOMING', {batchSize:$batchSize,concurrency:$concurrency,graph:$graph})";
        String check = "MATCH (n:A) WHERE n.id <> 0 RETURN n.partition as partition";

        runQuery(query, parParams());
        runQuery(check, row ->
                assertEquals(42, row.getNumber("partition").intValue()));
    }

    @Test
    public void shouldAllowCypherGraph() {
        String query = "CALL algo.labelPropagation('MATCH (n) RETURN id(n) as id, n.weight as weight, n.partition as value', 'MATCH (s)-[r:X]->(t) RETURN id(s) as source, id(t) as target, r.weight as weight', 'OUTGOING', {graph:'cypher',batchSize:$batchSize,concurrency:$concurrency})";
        runQuery(query, parParams(), row -> assertEquals(12, row.getNumber("nodes").intValue()));
    }

    @Test
    public void shouldStreamResults() {
        // this one deliberately tests the streaming and non streaming versions against each other to check we get the same results
        // we intentionally start with no labels defined for any nodes (hence partitionProperty = {lpa, lpa2})

        runQuery("CALL algo.labelPropagation(null, null, 'OUTGOING', {iterations: 20, partitionProperty: 'lpa'})", row -> {});

        String query = "CALL algo.labelPropagation.stream(null, null, {iterations: 20, direction: 'OUTGOING', partitionProperty: 'lpa2'}) " +
                "YIELD nodeId, label " +
                "MATCH (node) WHERE id(node) = nodeId " +
                "RETURN node.id AS id, id(node) AS internalNodeId, node.lpa AS partition, label";

        runQuery(query, row -> {
            assertEquals(row.getNumber("partition").intValue(), row.getNumber("label").intValue());
        });
    }

    private void runQuery(String query, Map<String,  Object> params) {
        runQuery(query, params, row -> {});
    }

    private void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        runQuery(query, Collections.emptyMap(), check);
    }

    private void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    private Map<String, Object> parParams() {
        return MapUtil.map("batchSize", parallel ? 1 : 100, "concurrency", parallel ? 8: 1, "graph", graphImpl);
    }
}
