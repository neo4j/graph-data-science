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
import org.neo4j.gds.core.Username;

import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Notice how we are just testing what the procedure actually does:
 * - Detects operator username and status
 * - Turns parameter defaults from null to Optional
 * - Delegates to facade
 *
 * Menial tasks, just like we want here at the edge of our software. Unstack all the nasty dependencies we have to
 * interact with because procedure framework; marshall user input; establish invariants.
 *
 * This way, down in the facade, we can cleanly test all the combinations of things that make up the actual business
 * rules, like the one about how if you are not an admin you can't list other people's settings.
 */
class DefaultsAndLimitsConfigurationProcedureTest {
    @Test
    void shouldListGlobalDefaults() {
        var facade = mock(DefaultsAndLimitsConfigurationFacade.class);
        var procedure = new DefaultsAndLimitsConfigurationProcedure(
            null,
            Username.of("some administrator"),
            () -> true,
            facade
        );

        when(facade.listDefaults("some administrator", true, Optional.empty(), Optional.empty()))
            .thenReturn(Stream.of(new DefaultSetting("a", 42), new DefaultSetting("b", 87)));
        Stream<DefaultSetting> settings = procedure.listDefaults(null, null);

        assertThat(settings).satisfiesExactly(
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
    void shouldListPersonalDefaultForSpecificKey() {
        var facade = mock(DefaultsAndLimitsConfigurationFacade.class);
        var procedure = new DefaultsAndLimitsConfigurationProcedure(
            null,
            Username.of("some administrator"),
            () -> true,
            facade
        );

        when(facade.listDefaults("some administrator", true, Optional.of("Jonas Vingegaard"), Optional.of("b")))
            .thenReturn(Stream.of(new DefaultSetting("b", 87)));
        Stream<DefaultSetting> settings = procedure.listDefaults("Jonas Vingegaard", "b");

        assertThat(settings).satisfiesExactly(
            ds -> {
                assertThat(ds.key).isEqualTo("b");
                assertThat(ds.value).isEqualTo(87);
            }
        );
    }

    @Test
    void shouldSetGlobalDefault() {
        var facade = mock(DefaultsAndLimitsConfigurationFacade.class);
        var procedure = new DefaultsAndLimitsConfigurationProcedure(
            null,
            Username.of("some administrator"),
            () -> true,
            facade
        );

        procedure.setDefault(null, "foo", 42);

        verify(facade).setDefault("some administrator", true, Optional.empty(), "foo", 42);
    }

    @Test
    void shouldSetPersonalDefault() {
        var facade = mock(DefaultsAndLimitsConfigurationFacade.class);
        var procedure = new DefaultsAndLimitsConfigurationProcedure(
            null,
            Username.of("some administrator"),
            () -> true,
            facade
        );

        procedure.setDefault("Jonas Vingegaard", "foo", 42);

        verify(facade).setDefault("some administrator", true, Optional.of("Jonas Vingegaard"), "foo", 42);
    }
}
