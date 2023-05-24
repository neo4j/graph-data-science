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
package org.neo4j.gds.core.write;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@ValueClass
public interface ExporterContext {

    GraphDatabaseService graphDatabaseAPI();

    @Nullable
    InternalTransaction internalTransaction();

    SecurityContext securityContext();


    final class ProcedureContextWrapper implements ExporterContext {
        private final Context procedureContext;

        ProcedureContextWrapper(Context procedureContext) { this.procedureContext = procedureContext; }

        @Override
        public GraphDatabaseAPI graphDatabaseAPI() {
            return procedureContext.graphDatabaseAPI();
        }

        @Override
        public @Nullable InternalTransaction internalTransaction() {
            return procedureContext.internalTransactionOrNull();
        }

        @Override
        public SecurityContext securityContext() {
            return procedureContext.securityContext();
        }
    }
}
