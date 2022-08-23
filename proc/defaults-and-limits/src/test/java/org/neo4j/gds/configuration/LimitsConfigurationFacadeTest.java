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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This layer does authorisation. Test that.
 */
class LimitsConfigurationFacadeTest {
    @Test
    void shouldListLimits() {
        var configuration = mock(LimitsConfiguration.class);
        var facade = new LimitsConfigurationFacade(configuration);

        when(configuration.list(Optional.of("Cecilie Uttrup Ludwig"), Optional.of("b"))).thenReturn(Map.of(
            "a",
            42,
            "b",
            87
        ));
        Stream<LimitSetting> limits = facade.listLimits(
            "operator",
            true,
            Optional.of("Cecilie Uttrup Ludwig"),
            Optional.of("b")
        );

        assertThat(limits).satisfiesExactly(
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
    void shouldAuthoriseEverythingForAdministratorWhenListing() {
        var facade = new LimitsConfigurationFacade(null);

        facade.assertAuthorisedToList(null, true, Optional.empty());
        facade.assertAuthorisedToList(null, true, Optional.of("Cecilie Uttrup Ludwig"));
        facade.assertAuthorisedToList("Cecilie Uttrup Ludwig", true, Optional.of("Cecilie Uttrup Ludwig"));
    }

    // Regular users can see global limits
    @Test
    void shouldAuthoriseWhenThereIsNothingToAuthoriseFor() {
        var facade = new LimitsConfigurationFacade(null);

        facade.assertAuthorisedToList(null, false, Optional.empty());
        facade.assertAuthorisedToList(null, true, Optional.empty());
    }

    @Test
    void shouldNotAuthoriseRegularUserSeeingLimitsForOtherUser() {
        var facade = new LimitsConfigurationFacade(null);

        try {
            facade.assertAuthorisedToList("Ludwig", false, Optional.of("Vingegaard"));

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("User 'Ludwig' not authorized to list limits for user 'Vingegaard'");
        }
    }

    @Test
    void shouldAuthoriseRegularUsersSeeingTheirOwnDefaultSettings() {
        var facade = new LimitsConfigurationFacade(null);

        facade.assertAuthorisedToList("Cecilie Uttrup Ludwig", false, Optional.of("Cecilie Uttrup Ludwig"));
    }

    @Test
    void shouldSetGlobalDefault() {
        var configuration = mock(LimitsConfiguration.class);
        var facade = new LimitsConfigurationFacade(configuration);

        facade.setLimit("some administrator", true, Optional.empty(), "foo", 42);

        verify(configuration).set("foo", 42, Optional.empty());
    }

    @Test
    void shouldDisallowRegularUserSettingGlobalDefault() {
        var facade = new LimitsConfigurationFacade(null);

        try {
            facade.setLimit("Cecilie Uttrup Ludwig", false, Optional.empty(), "foo", 42);

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("User 'Cecilie Uttrup Ludwig' not authorized to set global limits");
        }
    }

    @Test
    void shouldSetPersonalDefault() {
        var configuration = mock(LimitsConfiguration.class);
        var facade = new LimitsConfigurationFacade(configuration);

        facade.setLimit("some administrator", true, Optional.of("Jonas Vingegaard"), "foo", 42);

        verify(configuration).set("foo", 42, Optional.of("Jonas Vingegaard"));
    }

    @Test
    void shouldDisallowRegularUserSettingPersonalDefaultForOtherUser() {
        var facade = new LimitsConfigurationFacade(null);

        try {
            facade.setLimit("Ludwig", false, Optional.of("Vingegaard"), "foo", 42);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("User 'Ludwig' not authorized to set limits for user 'Vingegaard'");
        }
    }

    // you cannot unilaterally override limits set on you
    @Test
    void shouldDisallowRegularUserSettingPersonalDefaultForThemself() {
        var configuration = mock(LimitsConfiguration.class);
        var facade = new LimitsConfigurationFacade(configuration);

        try {
            facade.setLimit("Ludwig", false, Optional.of("Ludwig"), "foo", 42);

            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage())
                .isEqualTo("User 'Ludwig' not authorized to set limits for user 'Ludwig'");
        }
    }
}
