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
package org.neo4j.gds.similarity.cosine;

import org.neo4j.gds.impl.similarity.CosineAlgorithm;
import org.neo4j.gds.impl.similarity.CosineConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.similarity.AlphaSimilaritySummaryResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class CosineWriteProc extends CosineProc<AlphaSimilaritySummaryResult> {

    private static final String DESCRIPTION = "Cosine-similarity is an algorithm for finding similar nodes based on the cosine similarity metric.";

    @Procedure(name = "gds.alpha.similarity.cosine.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<AlphaSimilaritySummaryResult> cosineWrite(
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(configuration);
    }

    @Override
    public ComputationResultConsumer<CosineAlgorithm, SimilarityAlgorithmResult, CosineConfig, Stream<AlphaSimilaritySummaryResult>> computationResultConsumer() {
        return writeResultConsumer();
    }
}
