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
package org.neo4j.gds.cypher;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.common.Edition;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.gds.ProcPreconditions;
import org.neo4j.gds.catalog.CatalogProc;
import org.neo4j.gds.compat.StorageEngineProxy;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.storageengine.InMemoryDatabaseCreator;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.READ;

public class GraphCreateCypherDbProc extends CatalogProc {

    private static final String DESCRIPTION = "Creates a database from a GDS graph.";

    @Procedure(name = "gds.alpha.create.cypherdb", mode = READ)
    @Description(DESCRIPTION)
    public Stream<CreateCypherDbResult> createDb(
        @Name(value = "dbName") String dbName,
        @Name(value = "graphName") String graphName
    ) {
        ProcPreconditions.check();
        validateGraphName(graphName);

        CreateCypherDbResult result = runWithExceptionLogging(
            "In-memory Cypher database creation failed",
            () -> {
                validateNeo4jEnterpriseEdition();
                MutableLong createMillis = new MutableLong(0);
                try (ProgressTimer ignored = ProgressTimer.start(createMillis::setValue)) {
                    InMemoryDatabaseCreator.createDatabase(api, username(), newDatabaseId(), graphName, dbName);
                }

                return new CreateCypherDbResult(dbName, graphName, createMillis.getValue());
            }
        );

        return Stream.of(result);
    }

    @SuppressWarnings("unused")
    public static class CreateCypherDbResult {
        public final String dbName;
        public final String graphName;
        public final long createMillis;

        public CreateCypherDbResult(String dbName, String graphName, long createMillis) {
            this.dbName = dbName;
            this.graphName = graphName;
            this.createMillis = createMillis;
        }
    }

    private void validateNeo4jEnterpriseEdition() {
        var edition = StorageEngineProxy.dbmsEdition(api);
        if (!(edition == Edition.ENTERPRISE)) {
            throw new DatabaseManagementException(formatWithLocale(
                "Requires Neo4j %s version, but found %s",
                Edition.ENTERPRISE,
                edition
            ));
        }
    }
}
