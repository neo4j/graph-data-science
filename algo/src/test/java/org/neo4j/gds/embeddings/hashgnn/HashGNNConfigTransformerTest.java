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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HashGNNConfigTransformerTest {

    @Test
    void shouldConvertCorrectly(){
        int iters = 1;
        int embDensity = 2;
        double neighInfluence = 3.0;
        int  output = 4;
        int binDimension = 5;
        double threshold = 6.0;
        int densityLevel = 7;
        int genDimension = 8;

        var binConfig = mock(BinarizeFeaturesConfig.class);
        when(binConfig.dimension()).thenReturn(binDimension);
        when(binConfig.threshold()).thenReturn(threshold);

        var genConfig =  mock(GenerateFeaturesConfig.class);
        when(genConfig.dimension()).thenReturn(genDimension);
        when(genConfig.densityLevel()).thenReturn(densityLevel);

        var config = mock(HashGNNConfig.class);
        when(config.iterations()).thenReturn(iters);
        when(config.embeddingDensity()).thenReturn(embDensity);
        when(config.neighborInfluence()).thenReturn(neighInfluence);
        when(config.outputDimension()).thenReturn(Optional.of(output));
        when(config.binarizeFeatures()).thenReturn(Optional.of(binConfig));
        when(config.generateFeatures()).thenReturn(Optional.of(genConfig));

        var params = HashGNNConfigTransformer.toParameters(config);

        assertThat(params.iterations()).isEqualTo(iters);
        assertThat(params.embeddingDensity()).isEqualTo(embDensity);
        assertThat(params.neighborInfluence()).isEqualTo(neighInfluence);
        assertThat(params.outputDimension().orElse(-1)).isEqualTo(output);
        assertThat(params.binarizeFeatures().orElseThrow()).satisfies(  bin ->{
            assertThat(bin.dimension()).isEqualTo(binDimension);
            assertThat(bin.threshold()).isEqualTo(threshold);
        });
        assertThat(params.generateFeatures().orElseThrow()).satisfies(  gen ->{
            assertThat(gen.dimension()).isEqualTo(genDimension);
            assertThat(gen.densityLevel()).isEqualTo(densityLevel);
        });

    }

}
