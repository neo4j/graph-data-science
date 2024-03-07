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
package org.neo4j.gds.procedures.algorithms.configuration;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.Default;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitFactory;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ConfigurationParserTest {

    @Test
    void shouldApplyDefaults() {
        var defaultsMock = mock(DefaultsConfiguration.class);
        when(defaultsMock.apply(any(), any())).thenReturn(Map.of("concurrency", 8, "sudo", true));

        var configurationParser = new ConfigurationParser(defaultsMock, null);

        Map<String, Object> userInput = Map.of("concurrency", 4);

        var updatedUserInput = configurationParser.applyDefaults(userInput, "bogus");

        assertThat(updatedUserInput)
            .hasSize(2)
            .containsEntry("concurrency", 8)
            .containsEntry("sudo", true);

        verify(defaultsMock, times(1)).apply(userInput, "bogus");
        verifyNoMoreInteractions(defaultsMock);
    }

    @Test
    void shouldCheckLimits() {
        var limitsConfiguration = new LimitsConfiguration(Map.of("concurrency", LimitFactory.create(8L)), Collections.emptyMap());

        var configurationParser = new ConfigurationParser(null, limitsConfiguration);

        var algorithmConfigurationMock = mock(AlgoBaseConfig.class);
        when(algorithmConfigurationMock.configKeys()).thenReturn(List.of("concurrency", "sudo"));

        Map<String, Object> userInputWithDefaults = Map.of("concurrency", 87L, "sudo", true);

        assertThatIllegalArgumentException().isThrownBy(() ->
            configurationParser.validateLimits(algorithmConfigurationMock, "bogus", userInputWithDefaults)
        ).withMessage("Configuration parameter 'concurrency' with value '87' exceeds it's limit of '8'");
    }

    @Test
    void shouldIgnoreIrrelevantLimits() {
        var limitsConfiguration = new LimitsConfiguration(
            Map.of("concurrency", LimitFactory.create(8L), "sudo", LimitFactory.create(true)),
            Collections.emptyMap()
        );

        var configurationParser = new ConfigurationParser(null, limitsConfiguration);

        var algorithmConfigurationMock = mock(AlgoBaseConfig.class);
        when(algorithmConfigurationMock.configKeys()).thenReturn(List.of("concurrency"));

        Map<String, Object> userInputWithDefaults = Map.of("concurrency", 8L, "sudo", false);

        assertThatNoException().isThrownBy(() -> configurationParser.validateLimits(
            algorithmConfigurationMock,
            "bogus",
            userInputWithDefaults
        ));
    }

    @Test
    void shouldComplainAboutArbitraryFields() {
        var configurationParser = new ConfigurationParser(null, null);

        var algorithmConfigurationMock = mock(AlgoBaseConfig.class);
        when(algorithmConfigurationMock.configKeys()).thenReturn(List.of("concurrency"));

        Map<String, Object> userInput = Map.of("concurrency", 8L, "sudo", false);

        assertThatException().isThrownBy(() -> configurationParser.validateOriginalConfig(
            userInput,
            algorithmConfigurationMock.configKeys()
        )).withMessageContaining("Unexpected configuration key: sudo");

    }

    @Test
    void shouldParseConfigSuccessfully() {

        var configurationParser = new ConfigurationParser(
            new DefaultsConfiguration(
                Map.of("concurrency", new Default(3L), "bar", new Default(false)),
                Collections.emptyMap()
            ),
            new LimitsConfiguration(Map.of("concurrency", LimitFactory.create(4L)), Collections.emptyMap())
        );
        BiFunction<String, CypherMapWrapper, WccStreamConfig> configCreator = (__, cypherMapWrapper) -> WccStreamConfig.of(
            cypherMapWrapper);
        assertThat(configurationParser.produceConfig(Map.of(), configCreator, "foo").concurrency()).isEqualTo(3);

    }

    @Test
    void shouldComplainOfIrrelevantFields() {

        var configurationParser = new ConfigurationParser(
            new DefaultsConfiguration(
                Map.of("concurrency", new Default(3L), "conurrency", new Default(false)),
                Collections.emptyMap()
            ),
            new LimitsConfiguration(Map.of("concurrency", LimitFactory.create(4L)), Collections.emptyMap())
        );
        BiFunction<String, CypherMapWrapper, WccStreamConfig> configCreator = (__, cypherMapWrapper) -> WccStreamConfig.of(
            cypherMapWrapper);

        assertThatException().isThrownBy(() -> configurationParser.produceConfig(
            Map.of("conurrency", 20),
            configCreator,
            "bogus"
        )).withMessageContaining("Unexpected configuration key: conurrency (Did you mean [concurrency]?)");

    }

    @Test
    void shouldTakeIntoConsiderationPersonalLimitsAndPersonalDefaults() {

        var configurationParser = new ConfigurationParser(
            new DefaultsConfiguration(
                Map.of("concurrency", new Default(3L), "bar", new Default(false)),
                Map.of("bogus", Map.of("concurrency", new Default(1L)))
            ),
            new LimitsConfiguration(
                Map.of("concurrency", LimitFactory.create(2L)),
                Map.of("bogus", Map.of("concurrency", LimitFactory.create(1L)))
            )
        );
        BiFunction<String, CypherMapWrapper, WccStreamConfig> configCreator = (__, cypherMapWrapper) -> WccStreamConfig.of(
            cypherMapWrapper);
        assertThat(configurationParser.produceConfig(Map.of(), configCreator, "bogus").concurrency()).isEqualTo(1);

    }

    @Test
    void shouldTakeIntoConsiderationPersonalLimitsAndPersonalDefaultsAndThrow() {

        var configurationParser = new ConfigurationParser(
            new DefaultsConfiguration(
                Map.of("concurrency", new Default(4L), "bar", new Default(false)),
                Map.of("bogus", Map.of("concurrency", new Default(3L)))
            ),
            new LimitsConfiguration(
                Map.of("concurrency", LimitFactory.create(4L)),
                Map.of("bogus", Map.of("concurrency", LimitFactory.create(1L)))
            )
        );
        BiFunction<String, CypherMapWrapper, WccStreamConfig> configCreator = (__, cypherMapWrapper) -> WccStreamConfig.of(
            cypherMapWrapper);
        assertThatException().isThrownBy(() -> configurationParser.produceConfig(Map.of(), configCreator, "bogus"))
            .withMessage("Configuration parameter 'concurrency' with value '3' exceeds it's limit of '1'");

    }

}
