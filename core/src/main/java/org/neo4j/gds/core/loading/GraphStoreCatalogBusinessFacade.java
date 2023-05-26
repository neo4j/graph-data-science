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

import org.neo4j.gds.api.DatabaseId;

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

    public GraphStoreCatalogBusinessFacade(
        PreconditionsService preconditionsService,
        GraphNameValidationService graphNameValidationService,
        GraphStoreCatalogService graphStoreCatalogService
    ) {
        this.preconditionsService = preconditionsService;
        this.graphNameValidationService = graphNameValidationService;
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    public boolean graphExists(String username, DatabaseId databaseId, String graphName) {
        checkPreconditions();

        validateGraphNameNotBlank(graphName);

        return graphStoreCatalogService.graphExists(username, databaseId, graphName);
    }

    private void checkPreconditions() {
        preconditionsService.checkPreconditions();
    }

    /**
     * A hook for validating graph name, could broaden later
     */
    private void validateGraphNameNotBlank(String graphName) {
        graphNameValidationService.ensureIsNotBlank(graphName);
    }
}
