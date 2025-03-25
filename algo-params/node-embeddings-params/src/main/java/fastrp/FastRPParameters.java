/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package fastrp;

import org.neo4j.gds.AlgorithmParameters;
import org.neo4j.gds.annotation.Parameters;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.List;
import java.util.Optional;

@Parameters
public record FastRPParameters(
    List<String> featureProperties,
    List<Number> iterationWeights,
    int embeddingDimension,
    int propertyDimension,
    Optional<String> relationshipWeightProperty,
    float normalizationStrength,
    Number nodeSelfInfluence,
    Concurrency concurrency,
    Optional<Long> randomSeed
)  implements AlgorithmParameters { }
