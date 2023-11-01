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
package org.neo4j.gds.procedures.centrality;

import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.centrality.CentralityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.procedures.configparser.ConfigurationParser;

import java.util.Map;
import java.util.function.Function;

public class CentralityProcedureFacade {

    private final ConfigurationParser configurationParser;
    private final DatabaseId databaseId;
    private final User user;
    private final CentralityAlgorithmsMutateBusinessFacade mutateBusinessFacade;
    private final CentralityAlgorithmsStatsBusinessFacade statsBusinessFacade;
    private final CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade;
    private final CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade;
    private final ProcedureReturnColumns procedureReturnColumns;

    private final CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade;
    private final AlgorithmMetaDataSetter algorithmMetaDataSetter;

    public CentralityProcedureFacade(
        ConfigurationParser configurationParser,
        DatabaseId databaseId,
        User user,
        ProcedureReturnColumns procedureReturnColumns,
        CentralityAlgorithmsMutateBusinessFacade mutateBusinessFacade,
        CentralityAlgorithmsStatsBusinessFacade statsBusinessFacade,
        CentralityAlgorithmsStreamBusinessFacade streamBusinessFacade,
        CentralityAlgorithmsWriteBusinessFacade writeBusinessFacade,
        CentralityAlgorithmsEstimateBusinessFacade estimateBusinessFacade,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        this.configurationParser = configurationParser;
        this.databaseId = databaseId;
        this.user = user;
        this.procedureReturnColumns = procedureReturnColumns;
        this.mutateBusinessFacade = mutateBusinessFacade;
        this.statsBusinessFacade = statsBusinessFacade;
        this.streamBusinessFacade = streamBusinessFacade;
        this.writeBusinessFacade = writeBusinessFacade;
        this.estimateBusinessFacade = estimateBusinessFacade;
        this.algorithmMetaDataSetter = algorithmMetaDataSetter;
    }




    // FIXME: the following two methods are duplicate, find a good place for them.
    private <C extends AlgoBaseConfig> C createStreamConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator
    ) {
        return createConfig(
            configuration,
            configCreator.andThen(algorithmConfiguration -> {
                algorithmMetaDataSetter.set(algorithmConfiguration);
                return algorithmConfiguration;
            })
        );
    }

    private <C extends AlgoBaseConfig> C createConfig(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configCreator
    ) {
        return configurationParser.produceConfig(configuration, configCreator, user.getUsername());
    }
    //FIXME: here ends the fixme-block
}
