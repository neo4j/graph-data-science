/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.assertj.ConditionFactory;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiClassNLRTrainProcTest extends BaseProcTest {

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(MultiClassNLRTrainProc.class);
        runQuery(createQuery());
    }

    @AfterEach
    void tearDown() {
        ModelCatalog.drop("", "model");
    }

    @Test
    void producesCorrectModel() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withNodeProperties(List.of("a", "b", "t"), DefaultValue.of(0D))
            .withAnyRelationshipType()
            .algo("gds.alpha.ml.node.logisticRegression")
            .trainMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", "model")
            .addParameter("featureProperties", List.of("a", "b"))
            .addParameter("targetProperty", "t")
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "trainMillis", greaterThan(0L),
            "modelInfo", ConditionFactory.containsExactlyInAnyOrderEntriesOf(Map.of(
                "name", "model",
                "type", "multiClassNodeLogisticRegression"
            )),
            "configuration", isA(Map.class)
        )));

        assertTrue(ModelCatalog.exists("", "model"));
        Model<?, ?> model = ModelCatalog.list("", "model");
        assertThat(model.algoType()).isEqualTo("multiClassNodeLogisticRegression");
    }

    public String createQuery() {
        return "CREATE " +
               "(n1:N {a: 2.0, b: 1.2, t: 1.0})," +
               "(n2:N {a: 1.3, b: 0.5, t: 0.0})," +
               "(n3:N {a: 0.0, b: 2.8, t: 0.0})," +
               "(n4:N {a: 1.0, b: 0.9, t: 1.0})";
    }

}
