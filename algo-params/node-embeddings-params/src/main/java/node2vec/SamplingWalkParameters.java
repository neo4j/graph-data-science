/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package node2vec;

import org.neo4j.gds.annotation.Parameters;

import java.util.List;

@Parameters
public record SamplingWalkParameters(
    List<Long> sourceNodes,
    int walksPerNode,
    int walkLength,
    double returnFactor,
    double inOutFactor,
    double positiveSamplingFactor,
    double negativeSamplingExponent,
    int walkBufferSize
) {
}
