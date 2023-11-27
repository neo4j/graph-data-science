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
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SimilaritiesFuncWithCypherTest extends BaseTest {

    @BeforeEach
    void setUp() throws Exception {
        GraphDatabaseApiProxy.registerFunctions(db, SimilaritiesFunc.class);
    }

    @Test
    void testCosineFunction() {
        runQueryWithResultConsumer(
            "RETURN gds.similarity.cosine([1.0, 0.5], [0.5, 0.5]) AS score",
            result -> assertEquals(0.75 / Math.sqrt(0.625), result.next().get("score"))
        );
    }

    @Test
    void testEuclideanFunction() {
        runQueryWithResultConsumer(
            "RETURN gds.similarity.euclidean([1.0, 0.5], [0.5, 0.5]) AS score",
            result -> assertEquals(1.0 / (1.0 + Math.sqrt(0.25 + 0)), result.next().get("score"))
        );
    }

    @Test
    void testEuclideanDistanceFunction() {
        runQueryWithResultConsumer(
            "RETURN gds.similarity.euclideanDistance([1.0, 0.5], [0.5, 0.5]) AS score",
            result -> assertEquals(Math.sqrt(0.25 + 0), result.next().get("score"))
        );
    }

    @Test
    void testPearsonFunction() {
        runQueryWithResultConsumer(
            "RETURN gds.similarity.pearson([1.0, 0.5], [0.5, 0.5]) AS score",
            result -> assertEquals(0.0, result.next().get("score"))
        );
    }

    @Test
    void testOverlapFunction() {
        runQueryWithResultConsumer(
            "RETURN gds.similarity.overlap([1, 5], [5, 5]) AS score",
            result -> assertEquals(1.0 / 2.0, result.next().get("score"))
        );
    }

    @Test
    void testJaccardFunction() {
        runQueryWithResultConsumer(
            "RETURN gds.similarity.jaccard([1, 5], [5, 5]) AS score",
            result -> assertEquals(1.0 / 3.0, result.next().get("score"))
        );
    }

    @Test
    void testJaccardFunctionWithInputFromDatabase() {
        assertThatNoException().isThrownBy(
            () -> runQueryWithResultConsumer(
                "CREATE (t:Test {listone: [1, 5], listtwo: [5, 5]}) RETURN gds.similarity.jaccard(t.listone, t.listtwo) AS score",
                result -> {
                    assertThat(result.hasNext()).isTrue();
                    var score = result.next().get("score");
                    assertThat(score)
                        .asInstanceOf(DOUBLE)
                        .isEqualTo(1.0 / 3.0);
                }
            )
        );
    }
}
