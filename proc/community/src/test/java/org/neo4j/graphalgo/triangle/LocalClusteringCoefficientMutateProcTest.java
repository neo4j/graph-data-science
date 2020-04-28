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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.values.storable.NumberType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.graphalgo.ElementProjection.PROJECT_ALL;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;

class LocalClusteringCoefficientMutateProcTest
    extends LocalClusteringCoefficientBaseProcTest<LocalClusteringCoefficientMutateConfig>
    implements GraphMutationTest<LocalClusteringCoefficient, LocalClusteringCoefficientMutateConfig, LocalClusteringCoefficient.Result> {

    @Override
    public String mutateProperty() {
        return "mutatedLocalCC";
    }

    @Override
    public NumberType mutatePropertyType() {
        return NumberType.FLOATING_POINT;
    }

    @Override
    public String expectedMutatedGraph() {
        return String.format(
            "  (a { mutatedLocalCC: %f })" +
            ", (b { mutatedLocalCC: %f })" +
            ", (c { mutatedLocalCC: %f })" +
            ", (d { mutatedLocalCC: %f })" +
            ", (e { mutatedLocalCC: %f })" +
            // Graph is UNDIRECTED, e.g. each rel twice
            ", (a)-->(b)" +
            ", (b)-->(a)" +
            ", (b)-->(c)" +
            ", (c)-->(b)" +
            ", (a)-->(c)" +
            ", (c)-->(a)" +

            ", (a)-->(d)" +
            ", (d)-->(a)" +
            ", (b)-->(d)" +
            ", (d)-->(b)" +

            ", (a)-->(e)" +
            ", (e)-->(a)" +
            ", (b)-->(e)" +
            ", (e)-->(b)" +

            ", (c)-->(d)" +
            ", (d)-->(c)",
                2.0 / 3, 2.0 / 3, 1.0, 1.0, 1.0
            );
    }

    @Test
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withRelationshipType(
                ALL_RELATIONSHIPS.name(),
                RelationshipProjection.of(PROJECT_ALL, Orientation.UNDIRECTED)
            )
            .algo("localClusteringCoefficient")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "averageClusteringCoefficient", closeTo(expectedAverageClusteringCoefficient() / 5, 1e-10),
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "mutateMillis", greaterThan(-1L),
            "nodePropertiesWritten", 5L
        )));
    }

    @Test
    void testMutateSeeded() {
        var query = "CALL gds.localClusteringCoefficient.mutate('g', {" +
                    "   mutateProperty: $mutateProperty," +
                    "   triangleCountProperty: 'seed'" +
                    "})";

        assertCypherResult(query, Map.of("mutateProperty", mutateProperty()), List.of(Map.of(
            "averageClusteringCoefficient", closeTo(expectedAverageClusteringCoefficientSeeded() / 5, 1e-10),
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "mutateMillis", greaterThan(-1L),
            "nodePropertiesWritten", 5L
        )));
    }

    @Override
    public Class<? extends AlgoBaseProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientMutateConfig>> getProcedureClazz() {
        return LocalClusteringCoefficientMutateProc.class;
    }

    @Override
    public LocalClusteringCoefficientMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return LocalClusteringCoefficientMutateConfig.of(
            getUsername(),
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }
}
