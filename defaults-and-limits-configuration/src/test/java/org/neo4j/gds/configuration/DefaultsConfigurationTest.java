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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void shouldListGlobalDefaults() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.empty(), Optional.empty());

        assertThat(settings.size()).isEqualTo(2);
        assertThat(settings.get("a")).isEqualTo(42);
        assertThat(settings.get("b")).isEqualTo(87);
    }

    @Test
    void shouldListPersonalDefaults() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.of("Jonas Vingegaard"), Optional.empty());

        assertThat(settings.size()).isEqualTo(4);
        assertThat(settings.get("a")).isEqualTo(42);
        assertThat(settings.get("b")).isEqualTo(87);
        assertThat(settings.get("c")).isEqualTo(117);
        assertThat(settings.get("d")).isEqualTo(23);
    }

    @Test
    void shouldListGlobalDefaultsForKey() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.empty(), Optional.of("a"));

        assertThat(settings.size()).isEqualTo(1);
        assertThat(settings.get("a")).isEqualTo(42);
    }

    @Test
    void shouldNotListKeysThatDoNotExist() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.empty(), Optional.of("e"));

        assertThat(settings).isEmpty();
    }

    @Test
    void shouldListPersonalDefaultsForKey() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.of("Jonas Vingegaard"), Optional.of("c"));

        assertThat(settings.size()).isEqualTo(1);
        assertThat(settings.get("c")).isEqualTo(117);
    }

    @Test
    void shouldOverlayPersonalDefaults() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87),
                "c", new Default(512)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "b", new Default(59),
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.of("Jonas Vingegaard"), Optional.empty());

        assertThat(settings.size()).isEqualTo(4);
        assertThat(settings.get("a")).isEqualTo(42);
        assertThat(settings.get("b")).isEqualTo(59);
        assertThat(settings.get("c")).isEqualTo(117);
        assertThat(settings.get("d")).isEqualTo(23);
    }

    @Test
    void shouldFilterOverlaidPersonalDefaults() {
        var defaults = new DefaultsConfiguration(
            Map.of(
                "a", new Default(42),
                "b", new Default(87),
                "c", new Default(512)
            ),
            Map.of(
                "Jonas Vingegaard",
                Map.of(
                    "b", new Default(59),
                    "c", new Default(117),
                    "d", new Default(23)
                )
            )
        );

        var settings = defaults.list(Optional.of("Jonas Vingegaard"), Optional.of("c"));

        assertThat(settings.size()).isEqualTo(1);
        assertThat(settings.get("c")).isEqualTo(117);
    }

    @Test
    void shouldSetGlobalDefault() {
        var configuration = new DefaultsConfiguration(new HashMap<>(), new HashMap<>());

        configuration.set("foo", 42, Optional.empty());

        Object value = configuration.list(Optional.empty(), Optional.empty()).get("foo");
        assertThat(value).isEqualTo(42);
    }

    @Test
    void shouldOverwriteGlobalDefault() {
        var configuration = new DefaultsConfiguration(new HashMap<>(), new HashMap<>());

        configuration.set("foo", 42, Optional.empty());
        configuration.set("foo", 87, Optional.empty());

        Object value = configuration.list(Optional.empty(), Optional.empty()).get("foo");
        assertThat(value).isEqualTo(87);
    }

    @Test
    void shouldSetPersonalDefault() {
        var configuration = new DefaultsConfiguration(new HashMap<>(), new HashMap<>());

        configuration.set("foo", 42, Optional.of("Jonas Vingegaard"));

        Object value = configuration.list(Optional.of("Jonas Vingegaard"), Optional.of("foo")).get("foo");
        assertThat(value).isEqualTo(42);

        assertThat(configuration.list(Optional.empty(), Optional.empty())).doesNotContainKey("foo");
    }

    @Test
    void shouldSupportDoubles() {
        var configuration = new DefaultsConfiguration(new HashMap<>(), Collections.emptyMap());

        configuration.set("foo", 3.14, Optional.empty());

        Object valueFromList = configuration.list(Optional.empty(), Optional.of("foo")).get("foo");
        assertThat(valueFromList).isEqualTo(3.14);

        Object valueFromApply = configuration.apply(new HashMap<>(), "Jonas Vingegaard").get("foo");
        assertThat(valueFromApply).isEqualTo(3.14);
    }

    @Test
    void shouldSupportBoolean() {
        var configuration = new DefaultsConfiguration(new HashMap<>(), Collections.emptyMap());

        configuration.set("foo", true, Optional.empty());

        Object valueFromList = configuration.list(Optional.empty(), Optional.of("foo")).get("foo");
        assertThat(valueFromList).isEqualTo(true);

        Object valueFromApply = configuration.apply(new HashMap<>(), "Jonas Vingegaard").get("foo");
        assertThat(valueFromApply).isEqualTo(true);
    }

    @Test
    void shouldSupportString() {
        var configuration = new DefaultsConfiguration(new HashMap<>(), Collections.emptyMap());

        configuration.set("foo", "bar", Optional.empty());

        Object valueFromList = configuration.list(Optional.empty(), Optional.of("foo")).get("foo");
        assertThat(valueFromList).isEqualTo("bar");

        Object valueFromApply = configuration.apply(new HashMap<>(), "Jonas Vingegaard").get("foo");
        assertThat(valueFromApply).isEqualTo("bar");
    }
}
