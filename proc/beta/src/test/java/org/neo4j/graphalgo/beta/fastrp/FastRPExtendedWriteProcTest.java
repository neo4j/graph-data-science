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
package org.neo4j.graphalgo.beta.fastrp;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.gds.WritePropertyConfigProcTest;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.WritePropertyConfigTest;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

class FastRPExtendedWriteProcTest extends FastRPExtendedProcTest<FastRPExtendedWriteConfig>
    implements WritePropertyConfigTest<FastRP, FastRPExtendedWriteConfig, FastRP.FastRPResult> {

    @TestFactory
    Stream<DynamicTest> configTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPExtendedWriteConfig>> getProcedureClazz() {
        return FastRPExtendedWriteProc.class;
    }

    @Override
    public FastRPExtendedWriteConfig createConfig(CypherMapWrapper userInput) {
        return FastRPExtendedWriteConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        CypherMapWrapper minimalConfig = super.createMinimalConfig(userInput);

        if (!minimalConfig.containsKey("writeProperty")) {
            return minimalConfig.withString("writeProperty", "embedding");
        }
        return minimalConfig;
    }

    @Test
    void shouldNotCrash() {
        int embeddingDimension = 128;
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL")
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0D))
            .algo("gds.beta.fastRPExtended")
            .writeMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyDimension", embeddingDimension/2)
            .addParameter("featureProperties", List.of("f1", "f2"))
            .addParameter("writeProperty", "embedding")
            .yields();

        runQuery(query);
    }
}
