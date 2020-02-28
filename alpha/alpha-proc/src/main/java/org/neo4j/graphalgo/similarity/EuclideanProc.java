/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.similarity;

import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.similarity.EuclideanAlgorithm;
import org.neo4j.graphalgo.impl.similarity.EuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.EuclideanConfigImpl;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.results.SimilarityResult;
import org.neo4j.graphalgo.results.SimilaritySummaryResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class EuclideanProc extends SimilarityProc<EuclideanAlgorithm, EuclideanConfig> {

    private static final String DESCRIPTION = "Euclidean-similarity is an algorithm for finding similar nodes based on the euclidean distance.";

    @Procedure(name = "gds.alpha.similarity.euclidean.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SimilarityResult> euclideanStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.similarity.euclidean.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<SimilaritySummaryResult> euclideanWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(graphNameOrConfig, configuration);
    }

    @Override
    protected EuclideanConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new EuclideanConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    EuclideanAlgorithm newAlgo(EuclideanConfig config) {
        return new EuclideanAlgorithm(config, api);
    }
}
