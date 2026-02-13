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
package org.neo4j.gds.procedures.algorithms.similarity.stream;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.similarity.SimilarityStreamResult;
import org.neo4j.gds.similarity.SimilarityComputeBusinessFacade;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnStreamConfig;
import org.neo4j.gds.similarity.knn.KnnStreamConfig;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackSimilarityStreamProcedureFacade {

    private final SimilarityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;

    public PushbackSimilarityStreamProcedureFacade(
        SimilarityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
    }

    public Stream<SimilarityStreamResult> knn(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, KnnStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.knn(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new KnnStreamResultTransformer(graphResources.graph())
        ).join();
    }

    public Stream<SimilarityStreamResult> filteredKnn(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, FilteredKnnStreamConfig::of);

        var parameters = config.toFilteredKnnParameters();
        return businessFacade.filteredKnn(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new FilteredKnnStreamResultTransformer(graphResources.graph())
        ).join();
    }
}
