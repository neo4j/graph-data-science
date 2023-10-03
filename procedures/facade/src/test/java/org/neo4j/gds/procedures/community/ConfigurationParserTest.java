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
package org.neo4j.gds.procedures.community;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitFactory;
import org.neo4j.gds.configuration.LimitsConfiguration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
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

        Map<String, Object> userInput = Map.of("concurrency", 87L);
        Map<String, Object> userInputWithDefaults = Map.of("concurrency", 8L, "sudo", true);

        assertThatIllegalArgumentException().isThrownBy(() ->
            configurationParser.validateLimits(algorithmConfigurationMock, "bogus", userInput, userInputWithDefaults)
        ).withMessage("Configuration parameter 'concurrency' with value '87' exceeds it's limit of '8'");
    }


}
