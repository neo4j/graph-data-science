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
package org.neo4j.gds.similarity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.functions.IsFiniteFunc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;

class EmbeddingsAsInputTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(CosineProc.class);
        registerFunctions(IsFiniteFunc.class);
        registerFunctions(AsNodeFunc.class);

        // create graph with kernel API as we can't write a float[]
        // as property using Cypher. It is equivalent to
        //   CREATE
        //     (a:Label { id:"1", embedding: [42F, 43F] })
        //   , (b:Label { id:"2", embedding: [44F, 45F] })
        TestSupport.fullAccessTransaction(db).accept((tx, ktx) -> {
            var tokens = ktx.tokenWrite();
            var label = tokens.labelGetOrCreateForName("Label");
            var id = tokens.propertyKeyGetOrCreateForName("id");
            var embedding = tokens.propertyKeyGetOrCreateForName("embedding");

            var data = ktx.dataWrite();
            var node1 = data.nodeCreate();
            data.nodeAddLabel(node1, label);
            data.nodeSetProperty(node1, id, Values.stringValue("1"));
            data.nodeSetProperty(node1, embedding, Values.floatArray(new float[]{42, 43}));

            var node2 = data.nodeCreate();
            data.nodeAddLabel(node2, label);
            data.nodeSetProperty(node2, id, Values.stringValue("2"));
            data.nodeSetProperty(node2, embedding, Values.floatArray(new float[]{44, 45}));
        });
    }

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
    }

    private static final String RUN_COSINE_STREAM =
            " WITH {item:id, weights: weights} AS userData" +
            " WITH collect(userData) AS data" +
            " CALL gds.alpha.similarity.cosine.stream({" +
            "   nodeProjection: '*'," +
            "   relationshipProjection: '*'," +
            "   similarityCutoff:0.5," +
            "   topK:5," +
            "   data: data" +
            " }) YIELD item1, item2, count1, count2, similarity" +
            " RETURN gds.util.asNode(item1).id AS from, gds.util.asNode(item2).id AS to, similarity" +
            " ORDER BY similarity DESC";

    @Test
    void cosineSimilarityWithEmbeddingResult() {
        var query = " MATCH (a:Label) WITH id(a) AS id, a.embedding AS weights" + RUN_COSINE_STREAM;

        runQueryWithResultConsumer(query, results -> {
            assertTrue(results.hasNext(), "Should have exactly two results");
            var firstItem = results.next();
            assertTrue(results.hasNext(), "Should have exactly two results");
            var secondItem = results.next();
            assertFalse(results.hasNext(), "Should have exactly two results");

            var firstSimilarity = ((Number) firstItem.get("similarity")).doubleValue();
            assertEquals(firstSimilarity, 0.999, 1e-3);
            var secondSimilarity = ((Number) secondItem.get("similarity")).doubleValue();
            assertEquals(secondSimilarity, 0.999, 1e-3);

            var firstFrom = String.valueOf(firstItem.get("from"));
            assertEquals(firstFrom, "1");
            var firstTo = String.valueOf(firstItem.get("to"));
            assertEquals(firstTo, "2");

            var secondFrom = String.valueOf(secondItem.get("from"));
            assertEquals(secondFrom, "2");
            var secondTo = String.valueOf(secondItem.get("to"));
            assertEquals(secondTo, "1");
        });
    }

    @Test
    void similarityFailsWithNonNumericInput() {
        var query = " MATCH (a:Label) WITH id(a) AS id, 'not a number' AS weights" + RUN_COSINE_STREAM;

        var exception = assertThrows(QueryExecutionException.class, () -> runQuery(query));
        var rootCause = rootCause(exception);
        assertAll(
            () -> assertTrue(rootCause instanceof IllegalArgumentException, () -> "exception thrown is " + rootCause.getClass()),
            () -> assertEquals("The weight input is not a list of numeric values, found instead: java.lang.String", rootCause.getMessage())
        );
    }

    @Test
    void similarityFailsWithNonNumericWeightElements() {
        var query = " MATCH (a:Label) WITH id(a) AS id, [42, 'not a number', 1337.0] AS weights" + RUN_COSINE_STREAM;

        var exception = assertThrows(QueryExecutionException.class, () -> runQuery(query));
        var rootCause = rootCause(exception);
        assertAll(
            () -> assertTrue(rootCause instanceof IllegalArgumentException, () -> "exception thrown is " + rootCause.getClass()),
            () -> assertEquals("The weight input contains a non-numeric value at index 1: not a number", rootCause.getMessage())
        );
    }
}
