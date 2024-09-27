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
package org.neo4j.gds.procedures.algorithms.similarity;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;

import java.util.Collections;
import java.util.Map;

public class SimilarityWriteResult {
    public final long preProcessingMillis;
    public final long computeMillis;
    public final long writeMillis;
    public final long postProcessingMillis;

    public final long nodesCompared;
    public final long relationshipsWritten;

    public final Map<String, Object> similarityDistribution;
    public final Map<String, Object> configuration;

    public SimilarityWriteResult(
        long preProcessingMillis,
        long computeMillis,
        long writeMillis,
        long postProcessingMillis,
        long nodesCompared,
        long relationshipsWritten,
        Map<String, Object> similarityDistribution,
        Map<String, Object> configuration
    ) {
        this.preProcessingMillis = preProcessingMillis;
        this.computeMillis = computeMillis;
        this.writeMillis = writeMillis;
        this.postProcessingMillis = postProcessingMillis;
        this.nodesCompared = nodesCompared;
        this.relationshipsWritten = relationshipsWritten;
        this.similarityDistribution = similarityDistribution;
        this.configuration = configuration;
    }

    static SimilarityWriteResult emptyFrom(AlgorithmProcessingTimings timings, Map<String, Object> configurationMap) {
        return new SimilarityWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            0,
            0,
            0,
            Collections.emptyMap(),
            configurationMap
        );
    }

    static SimilarityWriteResult from(
        AlgorithmProcessingTimings timings,
        RelationshipsWritten relationshipsWritten,
        Map<String, Object> similarityDistribution,
        long nodesCompared,
        Map<String, Object> configurationMap
    ) {
        return new SimilarityWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            timings.mutateOrWriteMillis,
            0,
            nodesCompared,
            relationshipsWritten.value(),
            similarityDistribution,
            configurationMap
        );
    }
}
