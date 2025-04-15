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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BetweennessCentralityBaseConfigTest {

    @Test
    void shouldCreateParamsCorrectlyWithSampling() {

        var config = mock(BetweennessCentralityBaseConfig.class);
        when(config.samplingSeed()).thenReturn(Optional.of(42L));
        when(config.samplingSize()).thenReturn(Optional.of(123L));
        when(config.hasRelationshipWeightProperty()).thenReturn(true);
        when(config.concurrency()).thenReturn(null);
        when(config.toParameters()).thenCallRealMethod();

        var params = config.toParameters();

        assertThat(params.hasRelationshipWeightProperty()).isTrue();
        var samplingParams = params.samplingParameters();
        assertThat(samplingParams).isNotEmpty();
        assertThat(samplingParams.get()).satisfies(samplingParameters -> {
                assertThat(samplingParameters.samplingSize()).isEqualTo(123L);
                assertThat(samplingParameters.seed().get()).isEqualTo(42L);
            }
        );
        assertThat(params.samplingSize().get()).isEqualTo(123L);

    }

    @Test
    void shouldCreateParamsCorrectlyWithoutSampling() {

        var config = mock(BetweennessCentralityBaseConfig.class);
        when(config.samplingSize()).thenReturn(Optional.empty());
        when(config.samplingSeed()).thenReturn(Optional.of(123L)); //ignored anyway
        when(config.hasRelationshipWeightProperty()).thenReturn(false);
        when(config.concurrency()).thenReturn(null);
        when(config.toParameters()).thenCallRealMethod();

        var params = config.toParameters();

        assertThat(params.hasRelationshipWeightProperty()).isFalse();
        var samplingParams = params.samplingParameters();
        assertThat(samplingParams).isEmpty();
        assertThat(params.samplingSize()).isEmpty();

    }

}
