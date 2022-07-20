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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.config.SeedConfig.SEED_PROPERTY_KEY;

class SeedConfigTest {

    @Test
    void testDefaultSeedPropertyIsNull() {
        var mapWrapper = CypherMapWrapper.empty();
        var config = new TestSeedConfigImpl(mapWrapper);
        assertThat(config.seedProperty()).isNull();
    }

    @Test
    void testEmptySeedPropertyValues() {
        var mapWrapper = CypherMapWrapper.empty().withString(SEED_PROPERTY_KEY, "");
        var config = new TestSeedConfigImpl(mapWrapper);
        assertThat(config.seedProperty()).isNull();
    }

    @Test
    void failOnBlankPropertyName() {
        var mapWrapper = CypherMapWrapper.empty().withString(SEED_PROPERTY_KEY, "  ");
        assertThatThrownBy(() -> new TestSeedConfigImpl(mapWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not end or begin with whitespace characters");
    }

    @Test
    void shouldFailWithInvalidSeedProperty() {
        var graphStore = GdlFactory.of("(a {bar: 42})").build();
        var config = TestSeedConfigImpl.builder().seedProperty("foo").build();

        assertThatThrownBy(() -> config.validateSeedProperty(graphStore, config.nodeLabelIdentifiers(graphStore), List.of()))
            .hasMessageContaining("Seed property `foo` not found in graph with node properties: [bar]");
    }

    @Configuration
    interface TestSeedConfig extends AlgoBaseConfig, SeedConfig {

    }
}
