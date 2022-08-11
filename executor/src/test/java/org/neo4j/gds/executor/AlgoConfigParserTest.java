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
package org.neo4j.gds.executor;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.configuration.Default;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.Limit;
import org.neo4j.gds.configuration.LimitsConfiguration;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

class AlgoConfigParserTest {
    @Test
    void shouldParseAlgoConfig() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap()),
            new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap())
        );

        var configuration = configurationParser.processInput(Map.of("bar", 42));

        assertThat(configuration.bar()).isEqualTo(42);
    }

    @Test
    void shouldCatchMissingParameters() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap()),
            new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap())
        );

        try {
            configurationParser.processInput(Map.of(/* no bar?! */));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("No value specified for the mandatory configuration parameter `bar`");
        }
    }

    @Test
    void shouldNotCatchMissingParametersWhenDefaultsSubstitutedIn() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Map.of("bar", new Default(42)), Collections.emptyMap()),
            new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap())
        );

        var configuration = configurationParser.processInput(Collections.emptyMap(/* no bar?! */));

        assertThat(configuration.bar()).isEqualTo(42);
    }

    @Test
    void shouldNotCatchMissingParametersWhenPersonalDefaultsSubstitutedIn() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(
                Collections.emptyMap(),
                Map.of("Jonas Vingegaard", Map.of("bar", new Default(42)))
            ),
            new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap())
        );

        var configuration = configurationParser.processInput(Collections.emptyMap(/* no bar?! */));

        assertThat(configuration.bar()).isEqualTo(42);
    }

    @Test
    void shouldCatchIncongruentParameters() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap()),
            new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap())
        );

        try {
            configurationParser.processInput(Map.of("bar", 42, "baz", 87));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isEqualTo("Unexpected configuration key: baz");
        }
    }

    @Test
    void shouldNotCatchIncongruentParametersComingFromDefaults() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Map.of("baz", new Default(87)), Collections.emptyMap()),
            new LimitsConfiguration(Collections.emptyMap(), Collections.emptyMap())
        );

        var configuration = configurationParser.processInput(Map.of("bar", 42));

        assertThat(configuration.bar()).isEqualTo(42);
    }

    @Test
    void shouldCheckLimits() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap()),
            new LimitsConfiguration(Map.of("bar", new Limit(42)), Collections.emptyMap())
        );

        try {
            configurationParser.processInput(Map.of("bar", 87));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("Configuration parameter 'bar' with value '87' exceeds it's limit of '42'");
        }
    }

    @Test
    void shouldNotCheckLimitsOnIrrelevantParameters() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<FooConfig>) (username, config) -> new FooConfigImpl(config),
            new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap()),
            new LimitsConfiguration(Map.of("baz", new Limit(23)), Collections.emptyMap())
        );

        try {
            configurationParser.processInput(Map.of("bar", 42, "baz", 87));

            fail();
        } catch (IllegalArgumentException e) {
            // the error is that the key is irrelevant, not that it violates a limit. and that is a good thing
            assertThat(e.getMessage()).isEqualTo("Unexpected configuration key: baz");
        }
    }

    @Test
    void shouldReportAllLimitViolationsInOneGo() {
        var configurationParser = new AlgoConfigParser<>(
            "Jonas Vingegaard",
            (NewConfigFunction<BarConfig>) (username, config) -> new BarConfigImpl(config),
            new DefaultsConfiguration(Collections.emptyMap(), Collections.emptyMap()),
            new LimitsConfiguration(
                Map.of(
                    "baz", new Limit(23),
                    "qux", new Limit(87)
                ),
                Collections.emptyMap()
            )
        );

        try {
            configurationParser.processInput(
                Map.of(
                    "baz", 42,
                    "qux", 117
                )
            );

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("Configuration exceeded multiple limits:\n" +
                           " - Configuration parameter 'baz' with value '42' exceeds it's limit of '23'\n" +
                           " - Configuration parameter 'qux' with value '117' exceeds it's limit of '87'");
        }
    }
}
