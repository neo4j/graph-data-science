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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.functions.NodePropertyFunc;
import org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.FLOAT_ARRAY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class FastRPMutateProcTest extends FastRPProcTest<FastRPMutateConfig>
    implements MutateNodePropertyTest<FastRP, FastRPMutateConfig, FastRP.FastRPResult> {

    @Override
    GdsCypher.ExecutionModes mode() {
        return GdsCypher.ExecutionModes.MUTATE;
    }

    @Override
    public String mutateProperty() {
        return "embedding";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.FLOAT_ARRAY;
    }

    @Override
    public String expectedMutatedGraph() {
        return null;
    }

    @Override
    public Optional<String> mutateGraphName() {
        return Optional.of("graphToMutate");
    }

    @BeforeEach
    void setupNodePropertyFunc() throws Exception {
        registerProcedures(GraphWriteNodePropertiesProc.class);
        registerFunctions(NodePropertyFunc.class);
        loadGraph(mutateGraphName().get());
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPMutateConfig, ?>> getProcedureClazz() {
        return FastRPMutateProc.class;
    }

    @Override
    public FastRPMutateConfig createConfig(CypherMapWrapper userInput) {
        return FastRPMutateConfig.of(userInput);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        CypherMapWrapper minimalConfig = super.createMinimalConfig(userInput);

        if (!minimalConfig.containsKey("mutateProperty")) {
            return minimalConfig.withString("mutateProperty", "embedding");
        }
        return minimalConfig;
    }

    @Override
    @Test
    public void testGraphMutation() {}

    @Override
    @Test
    public void testMutateFailsOnExistingToken() {}

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.fastrp.FastRPProcTest#weights")
    void shouldMutateNonZeroEmbeddings(List<Float> weights, double propertyRatio) {
        List<String> featureProperties = propertyRatio == 0 ? List.of() : List.of("f1", "f2");
        int embeddingDimension = 128;
        var mutateGraphName = mutateGraphName().orElseThrow();
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call(mutateGraphName)
            .algo("fastRP")
            .mutateMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", featureProperties)
            .addParameter("mutateProperty", mutateProperty());

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String query = queryBuilder.yields();

        runQuery(query);

        String expectedResultQuery = formatWithLocale(
            "MATCH (n:Node) RETURN gds.util.nodeProperty('%s', id(n), 'embedding') as embedding",
            mutateGraphName
        );

        runQueryWithRowConsumer(expectedResultQuery, row -> {
            assertThat(row.get("embedding"))
                .asInstanceOf(FLOAT_ARRAY)
                .hasSize(embeddingDimension)
                .matches(vector -> FloatVectorOperations.anyMatch(vector, v -> v != 0.0));
        });
    }

    @Test
    void shouldProduceEmbeddingsWithSpecificValues() {

            String graphCreateQuery = GdsCypher.call("g2labels")
                .graphProject()
                .withNodeLabel("Node")
                .withNodeLabel("Node2")
                .withRelationshipType("REL2", Orientation.UNDIRECTED)
                .withNodeProperties(List.of("f1","f2"), DefaultValue.of(0.0f))
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
            "MATCH (n) WHERE n:Node OR n:Node2 RETURN gds.util.nodeProperty('%s', id(n), 'embedding') as embedding, id(n) as nodeId",
            "g2labels"
        );
        var expectedEmbeddings = Map.of(
            0, new float[]{0.0f, -0.11861968f, -0.07284536f, -0.19146505f, 0.04577432f, -0.26431042f, 0.0f, 0.07284536f, -0.11861968f, 0.11861968f, 0.0f, 0.26431042f, 0.11861968f, -0.11861968f, 0.0f, 0.0f, 0.07284536f, 0.0f, -0.19146505f, -0.11861968f, -0.04577432f, 0.0f, -0.07284536f, -0.11861968f, 0.0f, -0.19146505f, 0.04577432f, 0.0f, 0.11861968f, -0.11861968f, 0.0f, 0.0f, 0.04577432f, -0.07284536f, -0.11861968f, -0.11861968f, 0.0f, 0.07284536f, 0.0f, 0.07284536f, -0.19146505f, 0.07284536f, 0.0f, 0.11861968f, -0.11861968f, 0.26431042f, 0.14569072f, 0.0f, 0.07284536f, 0.11861968f, 0.0f, -0.07284536f, -0.07284536f, -0.07284536f, 0.027071044f, 0.0f, -0.19146505f, 0.0f, 0.11861968f, -0.11861968f, 0.0f, 0.07284536f, -0.11861968f, 0.07284536f, 0.19062826f, -0.20042314f, -0.19062826f, -0.20042314f, 0.20042314f, -0.20042314f, 0.0f, 0.19062826f, -0.20042314f, 0.20042314f, 0.0f, 0.39105138f, 0.20042314f, -0.20042314f, 0.0f, 0.0f, 0.19062826f, 0.0f, -0.39105138f, -0.20042314f, -0.20042314f, 0.0f, 0.0f, -0.20042314f, -0.19062826f, -0.009794861f, 0.20042314f, 0.0f, 0.009794861f, -0.20042314f, 0.0f, 0.0f, 0.39105138f, -0.19062826f, -0.39105138f, -0.20042314f, 0.19062826f, -0.19062826f, 0.0f, 0.19062826f, -0.20042314f, 0.0f, 0.19062826f, 0.20042314f, -0.20042314f, 0.20042314f, 0.0f, 0.0f, 0.0f, 0.20042314f, 0.0f, 0.0f, 0.0f, 0.0f, -0.20042314f, 0.0f, -0.20042314f, -0.19062826f, 0.39105138f, -0.39105138f, 0.0f, 0.0f, -0.20042314f, 0.0f},
            1, new float[]{0.0f, -0.118619695f, -0.07284536f, -0.19146505f, 0.045774333f, -0.26431042f, 0.0f, 0.07284536f, -0.118619695f, 0.118619695f, 0.0f, 0.26431042f, 0.118619695f, -0.118619695f, 0.0f, 0.0f, 0.07284536f, 0.0f, -0.19146505f, -0.118619695f, -0.045774333f, 0.0f, -0.07284536f, -0.118619695f, 0.0f, -0.19146505f, 0.045774333f, 0.0f, 0.118619695f, -0.118619695f, 0.0f, 0.0f, 0.045774333f, -0.07284536f, -0.118619695f, -0.118619695f, 0.0f, 0.07284536f, 0.0f, 0.07284536f, -0.19146505f, 0.07284536f, 0.0f, 0.118619695f, -0.118619695f, 0.26431042f, 0.14569072f, 0.0f, 0.07284536f, 0.118619695f, 0.0f, -0.07284536f, -0.07284536f, -0.07284536f, 0.027071029f, 0.0f, -0.19146505f, 0.0f, 0.118619695f, -0.118619695f, 0.0f, 0.07284536f, -0.118619695f, 0.07284536f, 0.19062828f, -0.20042314f, -0.19062828f, -0.20042314f, 0.20042314f, -0.20042314f, 0.0f, 0.19062828f, -0.20042314f, 0.20042314f, 0.0f, 0.3910514f, 0.20042314f, -0.20042314f, 0.0f, 0.0f, 0.19062828f, 0.0f, -0.3910514f, -0.20042314f, -0.20042314f, 0.0f, 0.0f, -0.20042314f, -0.19062828f, -0.009794846f, 0.20042314f, 0.0f, 0.009794846f, -0.20042314f, 0.0f, 0.0f, 0.3910514f, -0.19062828f, -0.3910514f, -0.20042314f, 0.19062828f, -0.19062828f, 0.0f, 0.19062828f, -0.20042314f, 0.0f, 0.19062828f, 0.20042314f, -0.20042314f, 0.20042314f, 0.0f, 0.0f, 0.0f, 0.20042314f, 0.0f, 0.0f, 0.0f, 0.0f, -0.20042314f, 0.0f, -0.20042314f, -0.19062828f, 0.3910514f, -0.3910514f, 0.0f, 0.0f, -0.20042314f, 0.0f},
            2, new float[]{0.0f, -0.118619695f, -0.07284536f, -0.19146505f, 0.045774333f, -0.26431042f, 0.0f, 0.07284536f, -0.118619695f, 0.118619695f, 0.0f, 0.26431042f, 0.118619695f, -0.118619695f, 0.0f, 0.0f, 0.07284536f, 0.0f, -0.19146505f, -0.118619695f, -0.045774333f, 0.0f, -0.07284536f, -0.118619695f, 0.0f, -0.19146505f, 0.045774333f, 0.0f, 0.118619695f, -0.118619695f, 0.0f, 0.0f, 0.045774333f, -0.07284536f, -0.118619695f, -0.118619695f, 0.0f, 0.07284536f, 0.0f, 0.07284536f, -0.19146505f, 0.07284536f, 0.0f, 0.118619695f, -0.118619695f, 0.26431042f, 0.14569072f, 0.0f, 0.07284536f, 0.118619695f, 0.0f, -0.07284536f, -0.07284536f, -0.07284536f, 0.027071029f, 0.0f, -0.19146505f, 0.0f, 0.118619695f, -0.118619695f, 0.0f, 0.07284536f, -0.118619695f, 0.07284536f, 0.19062828f, -0.20042314f, -0.19062828f, -0.20042314f, 0.20042314f, -0.20042314f, 0.0f, 0.19062828f, -0.20042314f, 0.20042314f, 0.0f, 0.3910514f, 0.20042314f, -0.20042314f, 0.0f, 0.0f, 0.19062828f, 0.0f, -0.3910514f, -0.20042314f, -0.20042314f, 0.0f, 0.0f, -0.20042314f, -0.19062828f, -0.009794846f, 0.20042314f, 0.0f, 0.009794846f, -0.20042314f, 0.0f, 0.0f, 0.3910514f, -0.19062828f, -0.3910514f, -0.20042314f, 0.19062828f, -0.19062828f, 0.0f, 0.19062828f, -0.20042314f, 0.0f, 0.19062828f, 0.20042314f, -0.20042314f, 0.20042314f, 0.0f, 0.0f, 0.0f, 0.20042314f, 0.0f, 0.0f, 0.0f, 0.0f, -0.20042314f, 0.0f, -0.20042314f, -0.19062828f, 0.3910514f, -0.3910514f, 0.0f, 0.0f, -0.20042314f, 0.0f}
        );

        runQueryWithRowConsumer(expectedResultQuery, row -> {
            var currentNode = row.getNumber("nodeId").longValue();
            assertThat(row.get("embedding"))
                .asInstanceOf(FLOAT_ARRAY)
                .isEqualTo(expectedEmbeddings.get(Math.toIntExact(currentNode)));
        });
    }
}
