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
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.functions.NodePropertyFunc;
import org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations;
import org.neo4j.gds.GdsCypher;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
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
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPMutateConfig>> getProcedureClazz() {
        return FastRPMutateProc.class;
    }

    @Override
    public FastRPMutateConfig createConfig(CypherMapWrapper userInput) {
        return FastRPMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
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
    void shouldMutateNonZeroEmbeddings(List<Float> weights) {
        int embeddingDimension = 128;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .explicitCreation(mutateGraphName().get())
            .algo("fastRP")
            .mutateMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("mutateProperty", mutateProperty());

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String query = queryBuilder.yields();

        runQuery(query);

        String expectedResultQuery = formatWithLocale(
            "MATCH (n:Node) RETURN gds.util.nodeProperty('%s', id(n), 'embedding') as embedding",
            mutateGraphName().get()
        );

        runQueryWithRowConsumer(expectedResultQuery, row -> {
            assertThat((float[]) row.get("embedding"))
                .hasSize(embeddingDimension)
                .matches(vector -> FloatVectorOperations.anyMatch(vector, v -> v != 0.0));
        });
    }
}
