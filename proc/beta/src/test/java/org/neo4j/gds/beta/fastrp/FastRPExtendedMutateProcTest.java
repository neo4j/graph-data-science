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
package org.neo4j.gds.beta.fastrp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.functions.NodePropertyFunc;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.List;
import java.util.Optional;

class FastRPExtendedMutateProcTest extends FastRPExtendedProcTest<FastRPExtendedMutateConfig>
    implements MutateNodePropertyTest<FastRP, FastRPExtendedMutateConfig, FastRP.FastRPResult>  {

    @Override
    public String mutateProperty() {
        return "embedding";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.FLOAT_ARRAY;
    }

    @Override
    public Optional<String> mutateGraphName() {
        return Optional.of("graphToMutate");
    }

    @Override
    public String expectedMutatedGraph() {
        return null;
    }

    @BeforeEach
    void setupNodePropertyFunc() throws Exception {
        registerProcedures(GraphWriteNodePropertiesProc.class);
        registerFunctions(NodePropertyFunc.class);

        var graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL")
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0D))
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0D))
            .graphCreate(mutateGraphName().get())
            .yields();

        runQuery(graphCreateQuery);
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPExtendedMutateConfig>> getProcedureClazz() {
        return FastRPExtendedMutateProc.class;
    }

    @Override
    public FastRPExtendedMutateConfig createConfig(CypherMapWrapper userInput) {
        return FastRPExtendedMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
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
    @Disabled("This test does not work for FastRPExtended due to now expected graph")
    @Test
    public void testGraphMutation() {}

    @Override
    @Disabled("This test does not work for FastRPExtended due to now expected graph")
    @Test
    public void testMutateFailsOnExistingToken() {}

    @Test
    void shouldNotCrash() {
        int embeddingDimension = 128;
        double propertyRatio = 127.0/128;
        String query = GdsCypher.call()
            .explicitCreation(mutateGraphName().get())
            .algo("gds.beta.fastRPExtended")
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", List.of("f1", "f2"))
            .yields();

        runQuery(query);
    }
}
