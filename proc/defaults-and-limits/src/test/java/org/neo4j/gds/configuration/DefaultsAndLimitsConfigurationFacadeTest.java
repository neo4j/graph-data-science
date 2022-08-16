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

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultsAndLimitsConfigurationFacadeTest {
    @Test
    void shouldListDefaults() {
        var configuration = mock(DefaultsConfiguration.class);
        var facade = new DefaultsAndLimitsConfigurationFacade(configuration);

        when(configuration.list(Optional.empty(), Optional.empty())).thenReturn(Map.of("a", 42, "b", 87));
        Stream<DefaultSetting> defaults = facade.listDefaults("operator", true, Optional.empty(), Optional.empty());

        assertThat(defaults).satisfiesExactly(
            ds -> {
                assertThat(ds.key).isEqualTo("a");
                assertThat(ds.value).isEqualTo(42);
            },
            ds -> {
                assertThat(ds.key).isEqualTo("b");
                assertThat(ds.value).isEqualTo(87);
            }
        );
    }

    @Test
    void shouldAuthoriseEverythingForAdministrator() {
        var facade = new DefaultsAndLimitsConfigurationFacade(null);

        facade.assertAuthorised(null, true, Optional.empty());
        facade.assertAuthorised(null, true, Optional.of("Cecilie Uttrup Ludwig"));
        facade.assertAuthorised("Cecilie Uttrup Ludwig", true, Optional.of("Cecilie Uttrup Ludwig"));
    }

    // Regular users can see global defaults
    @Test
    void shouldAuthoriseWhenThereIsNothingToAuthoriseFor() {
        var facade = new DefaultsAndLimitsConfigurationFacade(null);

        facade.assertAuthorised(null, false, Optional.empty());
        facade.assertAuthorised(null, true, Optional.empty());
    }

    @Test
    void shouldNotAuthoriseRegularUserSeeingDefaultsForOtherUser() {
        var facade = new DefaultsAndLimitsConfigurationFacade(null);

        try {
            facade.assertAuthorised("Ludwig", false, Optional.of("Vingegaard"));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("User 'Ludwig' not authorized to list default settings for user 'Vingegaard'");
        }
    }

    @Test
    void shouldAuthoriseRegularUsersSeeingTheirOwnDefaultSettings() {
        var facade = new DefaultsAndLimitsConfigurationFacade(null);

        facade.assertAuthorised("Cecilie Uttrup Ludwig", false, Optional.of("Cecilie Uttrup Ludwig"));
    }
}
