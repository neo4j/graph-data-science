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
import java.util.Map;
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
class LimitsConfigurationProcedureTest {
    @Test
    void shouldMarshallConfigurationWhenListing() {
        var facade = mock(LimitsConfigurationFacade.class);
        var procedure = new LimitsConfigurationProcedureAdapter(facade, "Jonas Vingegaard", false);

        when(facade.listLimits("Jonas Vingegaard", false, Optional.of("Jonas Vingegaard"), Optional.of("b")))
            .thenReturn(Stream.of(new LimitSetting("b", 87)));
        Stream<LimitSetting> limits = procedure.listLimits(Map.of("username", "Jonas Vingegaard", "key", "b"));

        assertThat(limits).satisfiesExactly(
            ls -> {
                assertThat(ls.key).isEqualTo("b");
                assertThat(ls.value).isEqualTo(87);
            }
        );
    }

    @Test
    void shouldMarshallEmptyConfigurationWhenListing() {
        var facade = mock(LimitsConfigurationFacade.class);
        var procedure = new LimitsConfigurationProcedureAdapter(facade, "some administrator", false);

        when(facade.listLimits("some administrator", false, Optional.empty(), Optional.empty()))
            .thenReturn(Stream.of(new LimitSetting("a", 42), new LimitSetting("b", 87)));
        Stream<LimitSetting> limits = procedure.listLimits(Collections.emptyMap());

        assertThat(limits).satisfiesExactly(
            ls -> {
                assertThat(ls.key).isEqualTo("a");
                assertThat(ls.value).isEqualTo(42);
            },
            ls -> {
                assertThat(ls.key).isEqualTo("b");
                assertThat(ls.value).isEqualTo(87);
            }
        );
    }

    @Test
    void shouldMarshallConfigurationWhenSetting() {
        var facade = mock(LimitsConfigurationFacade.class);
        var procedure = new LimitsConfigurationProcedureAdapter(
            facade,
            "Jonas Vingegaard",
            false
        );

        procedure.setLimit("foo", 42, "Cecilie Uttrup Ludwig");

        verify(facade).setLimit("Jonas Vingegaard", false, Optional.of("Cecilie Uttrup Ludwig"), "foo", 42);
    }

    @Test
    void shouldMarshallEmptyConfigurationWhenSetting() {
        var facade = mock(LimitsConfigurationFacade.class);
        var procedure = new LimitsConfigurationProcedureAdapter(
            facade,
            "some administrator",
            true
        );

        procedure.setLimit("foo", 42, DefaultsAndLimitsConstants.DummyUsername);

        verify(facade).setLimit("some administrator", true, Optional.empty(), "foo", 42);
    }
}
