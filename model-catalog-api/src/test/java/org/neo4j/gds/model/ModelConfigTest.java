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
package org.neo4j.gds.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ModelConfigTest {

    @ParameterizedTest
    @ValueSource(strings = {"graph$", "+graph", "_?+", "my graph"})
    void validNames(String name) {
        assertThat(new TestModelConfigImpl("", CypherMapWrapper.create(Map.of("modelName", name))))
            .matches(config -> config.modelName().equals(name));
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "   graph", "graph ", " graph ", "\tgraph"})
    void failOnWhiteSpaces(String invalidName) {
        assertThatThrownBy(() -> new TestModelConfigImpl("", CypherMapWrapper.create(Map.of("modelName", invalidName))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("modelName")
            .hasMessageContaining("must not end or begin with whitespace characters");
    }

    @Test
    void nullOnEmptyString() {
        assertThatThrownBy(() -> new TestModelConfigImpl("", CypherMapWrapper.create(Map.of("modelName", ""))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No value specified for the mandatory configuration parameter `modelName`");
    }

    @Configuration
    interface TestModelConfig extends ModelConfig {}
}
