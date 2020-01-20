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
import org.neo4j.graphalgo.impl.similarity.CosineAlgorithm;
import org.neo4j.graphalgo.impl.similarity.CosineConfig;
import org.neo4j.graphalgo.impl.similarity.CosineConfigImpl;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.SimilarityResult;
import org.neo4j.graphalgo.results.SimilaritySummaryResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class CosineProc extends SimilarityProc<CosineAlgorithm, CosineConfig> {

    @Procedure(name = "gds.alpha.similarity.cosine.stream", mode = READ)
    public Stream<SimilarityResult> cosineStream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(graphNameOrConfig, configuration);
    }

    @Procedure(name = "gds.alpha.similarity.cosine.write", mode = WRITE)
    public Stream<SimilaritySummaryResult> cosineWrite(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(graphNameOrConfig, configuration);
    }

    @Override
    protected CosineConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper userInput
    ) {
        return new CosineConfigImpl(graphName, maybeImplicitCreate, username, userInput);
    }

    @Override
    CosineAlgorithm newAlgo(CosineConfig config) {
        return new CosineAlgorithm(config, api);
    }
}
