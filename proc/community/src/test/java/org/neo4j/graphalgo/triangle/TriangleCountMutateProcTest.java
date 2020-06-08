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
import org.neo4j.graphalgo.MutateNodePropertyTest;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.values.storable.NumberType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.graphalgo.ElementProjection.PROJECT_ALL;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;

class TriangleCountMutateProcTest
    extends TriangleCountBaseProcTest<TriangleCountMutateConfig>
    implements MutateNodePropertyTest<IntersectingTriangleCount, TriangleCountMutateConfig, IntersectingTriangleCount.TriangleCountResult> {

    @Override
    public String mutateProperty() {
        return "mutatedTriangleCount";
    }

    @Override
    public NumberType mutatePropertyType() {
        return NumberType.INTEGRAL;
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
    void testMutateYields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withRelationshipType(
                ALL_RELATIONSHIPS.name(),
                RelationshipProjection.of(PROJECT_ALL, Orientation.UNDIRECTED)
            )
            .algo("triangleCount")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 1L,
            "nodeCount", 3L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "mutateMillis", greaterThan(-1L),
            "nodePropertiesWritten", 3L
        )));
    }

    @Test
    void testMutateWithMaxDegree() {
        // Add a single node and connect it to the triangle
        // to be able to apply the maxDegree filter.
        runQuery("MATCH (n) " +
                 "WITH n LIMIT 1 " +
                 "CREATE (d)-[:REL]->(n)");

        var createQuery = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .graphCreate("testGraph")
            .yields();

        runQuery(createQuery);

        var query = GdsCypher.call()
            .explicitCreation("testGraph")
            .algo("triangleCount")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .addParameter("maxDegree", 2)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 0L,
            "nodeCount", 4L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class),
            "nodePropertiesWritten", 4L,
            "mutateMillis", greaterThan(-1L)
        )));

        Graph actualGraph = GraphStoreCatalog.get(getUsername(), "testGraph").graphStore().getUnion();

        assertGraphEquals(
            fromGdl(
                "  (a { mutatedTriangleCount: -1 })" +
                ", (b { mutatedTriangleCount: 0 })" +
                ", (c { mutatedTriangleCount: 0 })" +
                ", (d { mutatedTriangleCount: 0 })" +
                // Graph is UNDIRECTED, e.g. each rel twice
                ", (a)-->(b)" +
                ", (b)-->(a)" +
                ", (b)-->(c)" +
                ", (c)-->(b)" +
                ", (a)-->(c)" +
                ", (c)-->(a)" +
                ", (d)-->(a)" +
                ", (a)-->(d)"
            ), actualGraph);
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
