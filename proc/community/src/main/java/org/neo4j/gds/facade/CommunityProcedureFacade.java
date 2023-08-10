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
package org.neo4j.gds.facade;

import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfig;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserServices;
import org.neo4j.gds.wcc.WccMutateConfig;
import org.neo4j.gds.wcc.WccMutateResult;
import org.neo4j.gds.wcc.WccStreamConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import java.util.Map;
import java.util.stream.Stream;

public class CommunityProcedureFacade {
    private final AlgorithmsBusinessFacade algorithmsBusinessFacade;
    private final UserServices userServices;
    private final DatabaseIdService databaseIdService;

    private final GraphDatabaseService graphDatabaseService;

    private final SecurityContext securityContext;

    public CommunityProcedureFacade(
        AlgorithmsBusinessFacade algorithmsBusinessFacade,
        UserServices userServices,
        DatabaseIdService databaseIdService,
        GraphDatabaseService graphDatabaseService,
        SecurityContext securityContext
    ) {
        this.algorithmsBusinessFacade = algorithmsBusinessFacade;
        this.userServices = userServices;
        this.databaseIdService = databaseIdService;
        this.graphDatabaseService = graphDatabaseService;
        this.securityContext = securityContext;
    }

    // WCC

    public Stream<WccStreamResult> wccStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = WccStreamConfig.of(CypherMapWrapper.create(configuration));

        // This is needed because of `com.neo4j.gds.ProcedureSignatureGuard` 🤦
        algorithmMetaDataSetter.set(streamConfig);

        var computationResult = algorithmsBusinessFacade.wcc(
            graphName,
            streamConfig,
            user(),
            databaseId(),
            ProgressTracker.NULL_TRACKER
        );

        return WccComputationResultTransformer.toStreamResult(computationResult);
    }

    public Stream<WccMutateResult> wccMutate(String graphName, Map<String, Object> configuration) {
        var config = WccMutateConfig.of(CypherMapWrapper.create(configuration));

        var computationResult = algorithmsBusinessFacade.wcc(
            graphName,
            config,
            user(),
            databaseId(),
            ProgressTracker.NULL_TRACKER
        );

        return Stream.of(WccComputationResultTransformer.toMutateResult(computationResult, config));
    }

    // WCC end

    // K-Core Decomposition
    public Stream<KCoreStreamResult> kCoreStream(
        String graphName,
        Map<String, Object> configuration,
        AlgorithmMetaDataSetter algorithmMetaDataSetter
    ) {
        var streamConfig = KCoreDecompositionStreamConfig.of(CypherMapWrapper.create(configuration));

        // This is needed because of `com.neo4j.gds.ProcedureSignatureGuard` 🤦
        algorithmMetaDataSetter.set(streamConfig);

        var computationResult = algorithmsBusinessFacade.kCore(
            graphName,
            streamConfig,
            user(),
            databaseId(),
            ProgressTracker.NULL_TRACKER
        );

        return KCoreResultTransformer.toStreamResult(computationResult);
    }

    // K-Core Decomposition end

    /**
     * We need to obtain the database id at this point in time so that we can send it down stream to business logic.
     * The database id is specific to the procedure call and/ or timing (note to self, figure out which it is).
     */
    private DatabaseId databaseId() {
        return databaseIdService.getDatabaseId(graphDatabaseService);
    }

    /**
     * The user here is request scoped, so we resolve it now and pass it down stream
     *
     * @return
     */
    private User user() {
        return userServices.getUser(securityContext);
    }
}
