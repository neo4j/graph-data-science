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
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.NativeFactory;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class TriangleCountMutateProcTest
    extends TriangleCountBaseProcTest<TriangleCountMutateConfig>
    implements GraphMutationTest<IntersectingTriangleCount, TriangleCountMutateConfig, IntersectingTriangleCount.TriangleCountResult> {

    @Override
    public String mutateProperty() {
        return "mutatedTriangleCount";
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (a { mutatedTriangleCount: 1 })" +
            ", (b { mutatedTriangleCount: 1 })" +
            ", (c { mutatedTriangleCount: 1 })" +
            // Graph is UNDIRECTED, e.g. each rel twice
            ", (a)-->(b)" +
            ", (b)-->(a)" +
            ", (b)-->(c)" +
            ", (c)-->(b)" +
            ", (a)-->(c)" +
            ", (c)-->(a)";
    }

    @Test
    void testMutate() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("triangleCount")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .addParameter("sudo", true)
            .yields(
                "createMillis",
                "computeMillis",
                "mutateMillis",
                "postProcessingMillis",
                "communityDistribution",
                "configuration"
            );

        runQueryWithRowConsumer(
            query,
            row -> {
                assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat(-1L, lessThan(row.getNumber("mutateMillis").longValue()));
            }
        );

        var writeQuery = GdsCypher
            .call()
            .explicitCreation("g")
            .algo("triangleCount")
            .writeMode()
            .addParameter("writeProperty", mutateProperty())
            .addParameter("sudo", true)
            .yields();

        runQuery(writeQuery);

        var updatedGraph = new StoreLoaderBuilder().api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalOrientation(Orientation.UNDIRECTED)
            .addNodeProperty(mutateProperty(), mutateProperty(), 42.0, Aggregation.NONE)
            .build()
            .graph(NativeFactory.class);

        assertGraphEquals(fromGdl(expectedMutatedGraph()), updatedGraph);

    }

    @Override
    public Class<? extends AlgoBaseProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateConfig>> getProcedureClazz() {
        return TriangleCountMutateProc.class;
    }

    @Override
    public TriangleCountMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return TriangleCountMutateConfig.of(
            getUsername(),
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }


}
