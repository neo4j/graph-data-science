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
package org.neo4j.gds.embeddings.fastrp;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.functions.NodePropertyFunc;
import org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.FLOAT_ARRAY;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ExtendWith(SoftAssertionsExtension.class)
class FastRPMutateProcTest extends BaseProcTest {

    private static final String FAST_RP_GRAPH = "fastRpGraph";

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a', f1: 0.4, f2: 1.3})" +
        ", (b:Node {name: 'b', f1: 2.1, f2: 0.5})" +
        ", (e:Node2 {name: 'e'})" +
        ", (c:Isolated {name: 'c'})" +
        ", (d:Isolated {name: 'd'})" +
        ", (a)-[:REL]->(b)" +

        ", (a)<-[:REL2 {weight: 2.0}]-(b)" +
        ", (a)<-[:REL2 {weight: 1.0}]-(e)";


    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(
            FastRPMutateProc.class,
            GraphProjectProc.class
        );
        registerFunctions(NodePropertyFunc.class);

        var graphCreateQuery = GdsCypher.call(FAST_RP_GRAPH)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0.0f))
            .yields();
        runQuery(graphCreateQuery);
    }

    @ParameterizedTest
    @MethodSource("weights")
    void shouldMutateNonZeroEmbeddings(List<Float> weights, double propertyRatio) {

        var query = GdsCypher.call(FAST_RP_GRAPH)
            .algo("fastRP")
            .mutateMode()
            .addParameter("embeddingDimension", 128)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", List.of("f1", "f2"))
            .addParameter("mutateProperty", "embedding")
            .addParameter("iterationWeights", weights)
            .yields();

        runQuery(query);

        String expectedResultQuery = formatWithLocale(
            "MATCH (n:Node) RETURN gds.util.nodeProperty('%s', id(n), 'embedding') as embedding",
            FAST_RP_GRAPH
        );

        runQueryWithRowConsumer(expectedResultQuery, row -> {
            assertThat(row.get("embedding"))
                .asInstanceOf(FLOAT_ARRAY)
                .hasSize(128)
                .matches(vector -> FloatVectorOperations.anyMatch(vector, v -> v != 0.0));
        });
    }

    @Test
    void shouldMutateNonZeroEmbeddingsWithoutProperties() {

        var query = GdsCypher.call(FAST_RP_GRAPH)
            .algo("fastRP")
            .mutateMode()
            .addParameter("embeddingDimension", 128)
            .addParameter("propertyRatio", 0d)
            .addParameter("featureProperties", List.<String>of())
            .addParameter("mutateProperty", "embedding")
            .yields();

        runQuery(query);

        String expectedResultQuery = formatWithLocale(
            "MATCH (n:Node) RETURN gds.util.nodeProperty('%s', id(n), 'embedding') as embedding",
            FAST_RP_GRAPH
        );

        runQueryWithRowConsumer(expectedResultQuery, row -> {
            assertThat(row.get("embedding"))
                .asInstanceOf(FLOAT_ARRAY)
                .hasSize(128)
                .matches(vector -> FloatVectorOperations.anyMatch(vector, v -> v != 0.0));
        });
    }

    @Test
    void shouldProduceEmbeddingsWithSpecificValues(SoftAssertions softly) {

        String graphCreateQuery = GdsCypher.call("g2labels")
            .graphProject()
            .withNodeLabel("Node")
            .withNodeLabel("Node2")
            .withRelationshipType("REL2", Orientation.UNDIRECTED)
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0.0f))
            .yields();
        runQuery(graphCreateQuery);

        int embeddingDimension = 128;
        double propertyRatio = 0.5;
        String query = GdsCypher.call("g2labels")
            .algo("fastRP")
            .mutateMode()
            .addParameter("mutateProperty", "embedding")
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", List.of("f1", "f2"))
            .addParameter("randomSeed", 42)
            .yields();

        runQuery(query);

        String expectedResultQuery = formatWithLocale(
            "MATCH (n) WHERE n:Node OR n:Node2 RETURN gds.util.nodeProperty('%s', id(n), 'embedding') as embedding, n.name as node",
            "g2labels"
        );
        var expectedEmbeddings = Map.of(
            "a",
            new float[]{-0.07246187f, -0.20042537f, -0.07246187f, 0.0f, -0.1279635f, 0.07246187f, 0.055501625f, -0.016960248f, -0.07246187f, 0.0f, 0.14492375f, 0.1279635f, 0.07246187f, 0.0f, 0.0f, 0.0f, 0.07246187f, 0.0f, 0.0f, -0.1279635f, 0.07246187f, -0.07246187f, 0.0f, 0.07246187f, -0.1279635f, 0.0f, 0.20042537f, 0.14492375f, 0.0f, -0.1279635f, -0.1279635f, 0.20042537f, 0.0f, 0.055501625f, -0.07246187f, 0.0f, -0.07246187f, -0.055501625f, 0.07246187f, 0.0f, -0.055501625f, 0.1279635f, 0.07246187f, 0.055501625f, 0.07246187f, 0.0f, 0.07246187f, -0.07246187f, 0.1279635f, -0.07246187f, -0.07246187f, 0.0f, -0.07246187f, 0.0f, -0.055501625f, 0.27288723f, 0.1279635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.07246187f, 0.0f, 0.20258348f, -0.20335533f, -0.20258348f, -0.20335533f, 0.20335533f, -0.20335533f, 0.0f, 0.20258348f, -0.20335533f, 0.20335533f, 0.0f, 0.4059388f, 0.20335533f, -0.20335533f, 0.0f, 0.0f, 0.20258348f, 0.0f, -0.4059388f, -0.20335533f, -0.20335533f, 0.0f, 0.0f, -0.20335533f, -0.20258348f, -7.7185035E-4f, 0.20335533f, 0.0f, 7.7185035E-4f, -0.20335533f, 0.0f, 0.0f, 0.4059388f, -0.20258348f, -0.4059388f, -0.20335533f, 0.20258348f, -0.20258348f, 0.0f, 0.20258348f, -0.20335533f, 0.0f, 0.20258348f, 0.20335533f, -0.20335533f, 0.20335533f, 0.0f, 0.0f, 0.0f, 0.20335533f, 0.0f, 0.0f, 0.0f, 0.0f, -0.20335533f, 0.0f, -0.20335533f, -0.20258348f, 0.4059388f, -0.4059388f, 0.0f, 0.0f, -0.20335533f, 0.0f},
            "b",
            new float[]{-0.07246187f, -0.20042537f, -0.07246187f, 0.0f, -0.1279635f, 0.07246187f, 0.055501625f, -0.016960248f, -0.07246187f, 0.0f, 0.14492375f, 0.1279635f, 0.07246187f, 0.0f, 0.0f, 0.0f, 0.07246187f, 0.0f, 0.0f, -0.1279635f, 0.07246187f, -0.07246187f, 0.0f, 0.07246187f, -0.1279635f, 0.0f, 0.20042537f, 0.14492375f, 0.0f, -0.1279635f, -0.1279635f, 0.20042537f, 0.0f, 0.055501625f, -0.07246187f, 0.0f, -0.07246187f, -0.055501625f, 0.07246187f, 0.0f, -0.055501625f, 0.1279635f, 0.07246187f, 0.055501625f, 0.07246187f, 0.0f, 0.07246187f, -0.07246187f, 0.1279635f, -0.07246187f, -0.07246187f, 0.0f, -0.07246187f, 0.0f, -0.055501625f, 0.27288723f, 0.1279635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.07246187f, 0.0f, 0.20258348f, -0.20335533f, -0.20258348f, -0.20335533f, 0.20335533f, -0.20335533f, 0.0f, 0.20258348f, -0.20335533f, 0.20335533f, 0.0f, 0.4059388f, 0.20335533f, -0.20335533f, 0.0f, 0.0f, 0.20258348f, 0.0f, -0.4059388f, -0.20335533f, -0.20335533f, 0.0f, 0.0f, -0.20335533f, -0.20258348f, -7.7185035E-4f, 0.20335533f, 0.0f, 7.7185035E-4f, -0.20335533f, 0.0f, 0.0f, 0.4059388f, -0.20258348f, -0.4059388f, -0.20335533f, 0.20258348f, -0.20258348f, 0.0f, 0.20258348f, -0.20335533f, 0.0f, 0.20258348f, 0.20335533f, -0.20335533f, 0.20335533f, 0.0f, 0.0f, 0.0f, 0.20335533f, 0.0f, 0.0f, 0.0f, 0.0f, -0.20335533f, 0.0f, -0.20335533f, -0.20258348f, 0.4059388f, -0.4059388f, 0.0f, 0.0f, -0.20335533f, 0.0f},
            "e",
            new float[]{-0.07246187f, -0.20042537f, -0.07246187f, 0.0f, -0.1279635f, 0.07246187f, 0.055501625f, -0.016960248f, -0.07246187f, 0.0f, 0.14492375f, 0.1279635f, 0.07246187f, 0.0f, 0.0f, 0.0f, 0.07246187f, 0.0f, 0.0f, -0.1279635f, 0.07246187f, -0.07246187f, 0.0f, 0.07246187f, -0.1279635f, 0.0f, 0.20042537f, 0.14492375f, 0.0f, -0.1279635f, -0.1279635f, 0.20042537f, 0.0f, 0.055501625f, -0.07246187f, 0.0f, -0.07246187f, -0.055501625f, 0.07246187f, 0.0f, -0.055501625f, 0.1279635f, 0.07246187f, 0.055501625f, 0.07246187f, 0.0f, 0.07246187f, -0.07246187f, 0.1279635f, -0.07246187f, -0.07246187f, 0.0f, -0.07246187f, 0.0f, -0.055501625f, 0.27288723f, 0.1279635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.07246187f, 0.0f, 0.20258348f, -0.20335533f, -0.20258348f, -0.20335533f, 0.20335533f, -0.20335533f, 0.0f, 0.20258348f, -0.20335533f, 0.20335533f, 0.0f, 0.4059388f, 0.20335533f, -0.20335533f, 0.0f, 0.0f, 0.20258348f, 0.0f, -0.4059388f, -0.20335533f, -0.20335533f, 0.0f, 0.0f, -0.20335533f, -0.20258348f, -7.7185035E-4f, 0.20335533f, 0.0f, 7.7185035E-4f, -0.20335533f, 0.0f, 0.0f, 0.4059388f, -0.20258348f, -0.4059388f, -0.20335533f, 0.20258348f, -0.20258348f, 0.0f, 0.20258348f, -0.20335533f, 0.0f, 0.20258348f, 0.20335533f, -0.20335533f, 0.20335533f, 0.0f, 0.0f, 0.0f, 0.20335533f, 0.0f, 0.0f, 0.0f, 0.0f, -0.20335533f, 0.0f, -0.20335533f, -0.20258348f, 0.4059388f, -0.4059388f, 0.0f, 0.0f, -0.20335533f, 0.0f}
        );

        runQueryWithRowConsumer(expectedResultQuery, row -> {
            float[] expected = expectedEmbeddings.get(row.getString("node"));
            softly.assertThat(row.get("embedding"))
                .asInstanceOf(FLOAT_ARRAY)
                .containsExactly(expected, Offset.offset(1e-7f));

        });
    }

    private static Stream<Arguments> weights() {
        return crossArguments(
            () -> Stream.of(
                Arguments.of(List.of(1.0f, 1.0f, 2.0f, 4.0f))
            ),
            () -> Stream.of(
                Arguments.of(0.5f),
                Arguments.of(1f)
            )
        );
    }

}
