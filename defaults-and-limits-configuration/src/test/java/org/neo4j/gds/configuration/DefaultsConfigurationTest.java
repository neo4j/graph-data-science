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
package org.neo4j.gds.configuration;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

class DefaultsConfigurationTest {
    @Test
    void shouldNotApplyDefaultWhenNoneExists() {
        var defaults = new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap());

        Map<String, Object> configuration = Map.of("concurrency", 42);
        var configurationWithDefaults = defaults.apply(configuration, "Jonas Vingegaard");

        assertThat(configurationWithDefaults).containsEntry("concurrency", 42);
    }

    @Test
    void shouldNotApplyDefaultWhenValueProvided() {
        var defaults = new DefaultsConfiguration(
            Map.of("concurrency", new Default(42)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Default(23)))
        );

        Map<String, Object> configuration = Map.of("concurrency", 87);
        var configurationWithDefaults = defaults.apply(configuration, "Jonas Vingegaard");

        assertThat(configurationWithDefaults).containsEntry("concurrency", 87);
    }

    @Test
    void shouldApplyGlobalDefaultsWhenValueMissing() {
        var defaults = new DefaultsConfiguration(Map.of("concurrency", new Default(42)), Collections.emptyMap());

        Map<String, Object> configuration = Collections.emptyMap();
        var configurationWithDefaults = defaults.apply(configuration, "Jonas Vingegaard");

        assertThat(configurationWithDefaults).containsEntry("concurrency", 42);
    }

    @Test
    void shouldApplyPersonalDefaultsWhenValueMissing() {
        var defaults = new DefaultsConfiguration(
            Collections.emptyMap(),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Default(42)))
        );

        Map<String, Object> configuration = Collections.emptyMap();
        var configurationWithDefaults = defaults.apply(configuration, "Jonas Vingegaard");

        assertThat(configurationWithDefaults).containsEntry("concurrency", 42);
    }

    @Test
    void shouldApplyPersonalDefaultsOverGlobalDefaults() {
        var defaults = new DefaultsConfiguration(
            Map.of("concurrency", new Default(23)),
            Map.of("Jonas Vingegaard", Map.of("concurrency", new Default(42)))
        );

        Map<String, Object> configuration = Collections.emptyMap();
        var configurationWithDefaults = defaults.apply(configuration, "Jonas Vingegaard");

        assertThat(configurationWithDefaults).containsEntry("concurrency", 42);
    }

    // placeholder for future work
    @Disabled
    @Test
    void shouldApplyDefaultsOfAllKinds() {
        fail("TODO: int, long, double, boolean");
    }
}
