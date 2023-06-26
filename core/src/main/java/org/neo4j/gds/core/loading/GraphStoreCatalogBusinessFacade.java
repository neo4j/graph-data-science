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
package org.neo4j.gds.core.loading;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This layer is shared between Neo4j and other integrations. It is entry-point agnostic.
 * "Business facade" to distinguish it from "procedure facade" and similar.
 * <p>
 * Here we have just business logic: no Neo4j bits or other integration bits, just Java POJO things.
 * <p>
 * By nature business logic is going to be bespoke, so one method per logical thing.
 * Take {@link GraphStoreCatalogBusinessFacade#graphExists(String, org.neo4j.gds.api.DatabaseId, String)} for example:
 * pure expressed business logic that layers above will use in multiple places, but!
 * Any marshalling happens in those layers, not here.
 * <p>
 * General validations could go here, think "graph exists" or "graph name not blank".
 * Also, this is where you would put cross-cutting concerns, things that many pieces of business logic share.
 * Generally though, a facade is really handy for others to pull in as a single dependency,
 * not for hosting all teh codez. _Maybe_ you stick your business logic in here directly,
 * if it is just one line or two; let's not be religious.
 * Ideally though this is a facade over many individual pieces of business logic in separate classes,
 * or behind other facades (oh gosh turtles, turtles everywhere :scream:).
 */
public class GraphStoreCatalogBusinessFacade {
    // services
    private final PreconditionsService preconditionsService;
    private final GraphNameValidationService graphNameValidationService;

    // business logic
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final DropGraphService dropGraphService;
    private final ListGraphService listGraphService;

    public GraphStoreCatalogBusinessFacade(
        PreconditionsService preconditionsService,
        GraphNameValidationService graphNameValidationService,
        GraphStoreCatalogService graphStoreCatalogService,
        DropGraphService dropGraphService,
        ListGraphService listGraphService
    ) {
        this.preconditionsService = preconditionsService;
        this.graphNameValidationService = graphNameValidationService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.dropGraphService = dropGraphService;
        this.listGraphService = listGraphService;
    }

    public boolean graphExists(String username, DatabaseId databaseId, String graphName) {
        checkPreconditions();

        var validatedGraphName = graphNameValidationService.validate(graphName);

        return graphStoreCatalogService.graphExists(username, databaseId, validatedGraphName);
    }

    /**
     * @param failIfMissing enable validation that graphs exist before dropping them
     * @param databaseName  optional override
     * @param username      optional override
     * @throws IllegalArgumentException if a database name was null or blank or not a String
     */
    public List<GraphStoreWithConfig> dropGraph(
        Object graphNameOrListOfGraphNames,
        boolean failIfMissing,
        String databaseName,
        String username,
        DatabaseId currentDatabase,
        User operator
    ) {
        checkPreconditions();

        // general parameter consolidation
        // we imagine any new endpoints will follow the exact same parameter lists I guess, for now
        List<String> validatedGraphNames = parseGraphNameOrListOfGraphNames(graphNameOrListOfGraphNames);
        DatabaseId databaseId = currentDatabase.orOverride(databaseName);
        Optional<String> usernameOverride = User.parseUsernameOverride(username);

        return dropGraphService.compute(validatedGraphNames, failIfMissing, databaseId, operator, usernameOverride);
    }

    public List<Pair<GraphStoreWithConfig, Map<String, Object>>> listGraphs(
        User user,
        String graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        checkPreconditions();

        Optional<String> validatedGraphName = graphNameValidationService.validatePossibleNull(graphName);

        return listGraphService.list(user, validatedGraphName, includeDegreeDistribution, terminationFlag);
    }

    private void checkPreconditions() {
        preconditionsService.checkPreconditions();
    }

    /**
     * I wonder if other endpoint will also deliver an Object type to parse in this layer - we shall see
     */
    private List<String> parseGraphNameOrListOfGraphNames(Object graphNameOrListOfGraphNames) {
        return graphNameValidationService.validateSingleOrList(graphNameOrListOfGraphNames);
    }
}
