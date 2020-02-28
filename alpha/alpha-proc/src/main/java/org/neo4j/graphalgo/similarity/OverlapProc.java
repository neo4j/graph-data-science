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
import org.neo4j.graphalgo.impl.similarity.OverlapAlgorithm;
import org.neo4j.graphalgo.impl.similarity.OverlapConfig;
import org.neo4j.graphalgo.impl.similarity.OverlapConfigImpl;
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

public class OverlapProc extends SimilarityProc<OverlapAlgorithm, OverlapConfig> {

    private static final String DESCRIPTION = "Overlap-similarity is an algorithm for finding similar nodes based on the overlap coefficient.";

    @Procedure(name = "gds.alpha.similarity.overlap.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SimilarityResult> overlapStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.similarity.overlap.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<SimilaritySummaryResult> overlapWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(graphNameOrConfig, configuration);
    }

    @Override
    protected OverlapConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new OverlapConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    OverlapAlgorithm newAlgo(OverlapConfig config) {
        return new OverlapAlgorithm(config, api);
    }
}
