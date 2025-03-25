/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package hashgnn;

import org.neo4j.gds.annotation.Parameters;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.List;
import java.util.Optional;

@Parameters
public record HashGNNParameters(
    Concurrency concurrency,
    int iterations,
    int embeddingDensity,
    double neighborInfluence,
    List<String> featureProperties,
    boolean heterogeneous,
    Optional<Integer> outputDimension,
    Optional<BinarizeParameters> binarizeFeatures,
    Optional<GenerateParameters> generateFeatures,
    Optional<Long> randomSeed
) {
}
