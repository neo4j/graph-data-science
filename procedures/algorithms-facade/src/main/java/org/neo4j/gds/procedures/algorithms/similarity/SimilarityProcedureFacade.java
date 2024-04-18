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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.similarity.SimilarityAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.procedures.algorithms.runners.StreamModeAlgorithmRunner;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.similarity.SimilarityResult;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class SimilarityProcedureFacade {
    private final KnnMutateStub knnMutateStub;
    private final ApplicationsFacade applicationsFacade;
    private final StreamModeAlgorithmRunner streamModeAlgorithmRunner;

    private SimilarityProcedureFacade(
        KnnMutateStub knnMutateStub,
        ApplicationsFacade applicationsFacade,
        StreamModeAlgorithmRunner streamModeAlgorithmRunner
    ) {
        this.knnMutateStub = knnMutateStub;
        this.applicationsFacade = applicationsFacade;
        this.streamModeAlgorithmRunner = streamModeAlgorithmRunner;
    }

    public static SimilarityProcedureFacade create(
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        ProcedureReturnColumns procedureReturnColumns,
        StreamModeAlgorithmRunner streamModeAlgorithmRunner
    ) {
        var knnMutateStub = new KnnMutateStub(genericStub, applicationsFacade, procedureReturnColumns);

        return new SimilarityProcedureFacade(knnMutateStub, applicationsFacade, streamModeAlgorithmRunner);
    }

    public KnnMutateStub knnMutateStub() {
        return knnMutateStub;
    }

    public Stream<SimilarityResult> knnStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new KnnResultBuilderForStreamMode();

        return streamModeAlgorithmRunner.runStreamModeAlgorithm(
            graphName,
            configuration,
            KnnStreamConfig::of,
            resultBuilder,
            streamMode()::knn
        );
    }

    private SimilarityAlgorithmsStreamModeBusinessFacade streamMode() {
        return applicationsFacade.similarity().stream();
    }
}
