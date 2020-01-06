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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.SimilarityRecorder;

import java.util.function.Supplier;

public interface SimilarityAlgorithm<T extends SimilarityInput> {
    T[] prepareInputs(Object rawData, Double skipValue) throws Exception;
    double similarityCutoff();
    SimilarityComputer<T> similarityComputer(Double skipValue);
    SimilarityRecorder<T> similarityRecorder(SimilarityComputer<T> computer, ProcedureConfiguration configuration);
    Supplier<RleDecoder> createDecoderFactory(T input);
    SimilarityResult postProcess(SimilarityResult result);
    int topK();
}
