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

import hashgnn.BinarizeParameters;
import hashgnn.GenerateParameters;
import hashgnn.HashGNNParameters;

import java.util.Optional;

public final class HashGNNConfigTransformer {

    private HashGNNConfigTransformer() {}

    public static HashGNNParameters toParameters(HashGNNConfig config) {

        return new HashGNNParameters(
            config.concurrency(),
            config.iterations(),
            config.embeddingDensity(),
            config.neighborInfluence(),
            config.featureProperties(),
            config.heterogeneous(),
            config.outputDimension(),
            toBinarizeParameters(config.binarizeFeatures()),
            toGenerateParameters(config.generateFeatures()),
            config.randomSeed()
        );
    }

    private static Optional<BinarizeParameters> toBinarizeParameters(Optional<BinarizeFeaturesConfig> config){
        return config
            .map(  binarizeConfig ->  new BinarizeParameters(
                    binarizeConfig.dimension(),
                    binarizeConfig.threshold()
                )
            );
    }

    private static Optional<GenerateParameters> toGenerateParameters(Optional<GenerateFeaturesConfig> config){
        return config
            .map(generateConfig ->  new GenerateParameters(
                generateConfig.densityLevel(),
                generateConfig.dimension())
            );
    }
}
