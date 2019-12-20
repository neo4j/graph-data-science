/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.results.SimilaritySummaryResult;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanAlgorithm;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanConfig;
import org.neo4j.graphalgo.impl.similarity.modern.ModernEuclideanConfigImpl;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ModernEuclideanProc extends ModernSimilarityProc<ModernEuclideanAlgorithm, ModernEuclideanConfig> {

    @Procedure(name = "gds.alpha.similarity.euclidean.stream", mode = READ)
    public Stream<SimilarityResult> euclideanStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.similarity.euclidean.write", mode = Mode.WRITE)
    public Stream<SimilaritySummaryResult> euclideanWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(graphNameOrConfig, configuration);
    }

    @Override
    protected ModernEuclideanConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new ModernEuclideanConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    ModernEuclideanAlgorithm newAlgo(ModernEuclideanConfig config) {
        return new ModernEuclideanAlgorithm(config, api);
    }
}
