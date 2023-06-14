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
package org.neo4j.gds.catalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.core.loading.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.core.utils.warnings.UserLogEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.gds.logging.Log;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The top layer for the Neo4j integration side:
 * thin, dumb procedure stubs can have this context-injected and call exactly one method,
 * passing user input and an appropriate output marshaller.
 * <p>
 * The output marshaller determines how easy or not it will be to generate stubs, but let's take that another day.
 * <p>
 * Take the graph exists _function_: logically it is a quad of {"gds.graph.exists", READ, input string, output boolean},
 * everything else is details. Contrast with the graph-exists _procedure_, same as the function,
 * but output gets marshalled in a bespoke fashion.
 * <p>
 * Baby steps: we start here, extracting business logic, structuring marshalling,
 * getting a handle on parameters vs dependencies.
 * <p>
 * Note that we take in _only_ parameters. That's because everything else is a dependency (maybe not the best name),
 * but certainly something that is not necessary for the stubs to consider;
 * this facade will be an extension and can grab/ initialise/ resolve anything except the things the user passes in.
 * Username for example, turns out we resolve that, so no need to consider it a parameter.
 * Nice lovely decoupling innit when we can just focus on business logic.
 */
public class GraphStoreCatalogProcedureFacade {
    // services
    private final DatabaseIdService databaseIdService;
    private final GraphDatabaseService graphDatabaseService;
    private final KernelTransactionService kernelTransactionService;
    private final Log log;
    private final ProcedureTransactionService procedureTransactionService;
    private final SecurityContext securityContext;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final UsernameService usernameService;

    // business facade
    private final GraphStoreCatalogBusinessFacade businessFacade;

    public GraphStoreCatalogProcedureFacade(
        DatabaseIdService databaseIdService,
        GraphDatabaseService graphDatabaseService,
        KernelTransactionService kernelTransactionService,
        Log log,
        ProcedureTransactionService procedureTransactionService,
        SecurityContext securityContext,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        UsernameService usernameService,
        GraphStoreCatalogBusinessFacade businessFacade
    ) {
        this.databaseIdService = databaseIdService;
        this.graphDatabaseService = graphDatabaseService;
        this.kernelTransactionService = kernelTransactionService;
        this.log = log;
        this.procedureTransactionService = procedureTransactionService;
        this.securityContext = securityContext;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.usernameService = usernameService;

        this.businessFacade = businessFacade;
    }

    /**
     * Discussion: this is used by two stubs, with different output marshalling functions.
     * <p>
     * We know we should test {@link #graphExists(String)} in isolation because combinatorials.
     * <p>
     * Do we test the output marshallers?
     * <p>
     * Well if we need confidence, not for just box ticking.
     * Neo4j Procedure Framework requires POJOs of a certain shape,
     * so there is scope for writing ridiculous amounts of code if you fancy ticking boxes.
     */
    @SuppressWarnings("WeakerAccess")
    public <RETURN_TYPE> RETURN_TYPE graphExists(String graphName, Function<Boolean, RETURN_TYPE> outputMarshaller) {
        boolean graphExists = graphExists(graphName);

        return outputMarshaller.apply(graphExists);
    }

    boolean graphExists(String graphName) {
        // stripping off Neo4j bits
        String username = username();
        DatabaseId databaseId = databaseId();

        // no static access! we want to be able to test this stuff
        return businessFacade.graphExists(username, databaseId, graphName);
    }

    /**
     * Huh, we never did jobId filtering...
     */
    public Stream<UserLogEntry> queryUserLog(String jobId) {
        var userLogStore = userLogServices.getUserLogStore(databaseId());

        return userLogStore.query(username());
    }

    /**
     * We need to obtain the username at this point in time so that we can send it down stream to business logic.
     * The username is specific to the procedure call.
     */
    private String username() {
        return usernameService.getUsername(securityContext);
    }

    /**
     * We need to obtain the database id at this point in time so that we can send it down stream to business logic.
     * The database id is specific to the procedure call and/ or timing (note to self, figure out which it is).
     */
    private DatabaseId databaseId() {
        return databaseIdService.getDatabaseId(graphDatabaseService);
    }
}
