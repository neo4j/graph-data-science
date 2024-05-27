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
package org.neo4j.gds.procedures.integration;

import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;
import org.neo4j.gds.procedures.UserAccessor;
import org.neo4j.internal.kernel.api.security.SecurityContext;

/**
 * Lifecycle: global
 */
class PipelinesProcedureFacadeProvider {
    private final UserAccessor userAccessor = new UserAccessor();

    PipelinesProcedureFacade createPipelinesProcedureFacade(SecurityContext securityContext) {
        var user = userAccessor.getUser(securityContext);

        return new PipelinesProcedureFacade(user);
    }
}
