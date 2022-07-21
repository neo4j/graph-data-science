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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class TriangleCountMutateProcTest
    extends TriangleCountBaseProcTest<TriangleCountMutateConfig>
    implements MutateNodePropertyTest<IntersectingTriangleCount, TriangleCountMutateConfig, IntersectingTriangleCount.TriangleCountResult> {

    @Override
    public String mutateProperty() {
        return "mutatedTriangleCount";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.LONG;
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
            .call(DEFAULT_GRAPH_NAME)
            .algo("triangleCount")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 1L,
            "nodeCount", 3L,
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
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

        var createQuery = GdsCypher.call("testGraph")
            .graphProject()
            .loadEverything(Orientation.UNDIRECTED)
            .yields();

        runQuery(createQuery);

        var query = GdsCypher.call("testGraph")
            .algo("triangleCount")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .addParameter("maxDegree", 2)
            .yields("globalTriangleCount", "nodeCount", "nodePropertiesWritten");

        assertCypherResult(query, List.of(Map.of(
            "globalTriangleCount", 0L,
            "nodeCount", 4L,
            "nodePropertiesWritten", 4L
        )));

        Graph actualGraph = GraphStoreCatalog.get(getUsername(), databaseId(), "testGraph").graphStore().getUnion();

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
    public Class<? extends AlgoBaseProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateConfig, ?>> getProcedureClazz() {
        return TriangleCountMutateProc.class;
    }

    @Override
    public TriangleCountMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return TriangleCountMutateConfig.of(mapWrapper);
    }
}
