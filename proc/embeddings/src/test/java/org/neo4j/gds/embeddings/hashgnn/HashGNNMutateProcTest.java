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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.MutateNodePropertyTest;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class HashGNNMutateProcTest extends HashGNNProcTest<HashGNNMutateConfig> implements
    MutateNodePropertyTest<HashGNN, HashGNNMutateConfig, HashGNN.HashGNNResult> {

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        var minimalConfig = super.createMinimalConfig(userInput);

        if (!minimalConfig.containsKey("mutateProperty")) {
            return minimalConfig.withString("mutateProperty", "embedding");
        }
        return minimalConfig;
    }

    @Override
    public Class<? extends AlgoBaseProc<HashGNN, HashGNN.HashGNNResult, HashGNNMutateConfig, ?>> getProcedureClazz() {
        return HashGNNMutateProc.class;
    }

    @Override
    public HashGNNMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return HashGNNMutateConfig.of(mapWrapper);
    }

    @Override
    public String mutateProperty() {
        return "embedding";
    }

    @Override
    public ValueType mutatePropertyType() {
        return ValueType.DOUBLE_ARRAY;
    }

    @Override
    public String expectedMutatedGraph() {
        return "CREATE" +
        "  (a {f1: 1, f2: [0.0, 0.0], embedding: [1.0, 0.0, 0.0]})" +
        ", (b {f1: 0, f2: [1.0, 0.0], embedding: [0.0, 0.0, 1.0]})" +
        ", (c {f1: 0, f2: [0.0, 1.0], embedding: [0.0, 0.0, 1.0]})" +
        ", (b)-[:R1]->(a)" +
        ", (b)-[:R2]->(c)";
    }

    @Test
    void failsOnInvalidBinarizationKeys() {
        assertThatThrownBy(() -> {
            HashGNNMutateConfig.of(CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "foo",
                    "featureProperties", List.of("x"),
                    "binarizeFeatures", Map.of("dimension", 100, "treshold", 2.0),
                    "embeddingDensity", 4,
                    "iterations", 100
                )
            ));

        }).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unexpected configuration key: treshold (Did you mean [threshold]?)");
    }

    @Test
    void failsOnInvalidGenerateFeaturesKeys() {
        assertThatThrownBy(() -> {
            HashGNNMutateConfig.of(CypherMapWrapper.create(
                Map.of(
                    "generateFeatures", Map.of("dimension", 100, "densityElfen", 2),
                    "embeddingDensity", 4,
                    "iterations", 100
                )
            ));

        }).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No value specified for the mandatory configuration parameter `densityLevel` (a similar parameter exists: [densityElfen])");
    }
}
