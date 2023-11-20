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
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.Preconditions;
import org.neo4j.gds.storageengine.InMemoryDatabaseCreationCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.WRITE;

public class DropEphemeralDbProc extends BaseProc {

    private static final String DESCRIPTION = "Drop an ephemeral database backed by an in-memory graph";

    @Procedure(name = "gds.ephemeral.database.drop", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<DropEphemeralDbResult> dropInMemoryDatabase(
        @Name(value = "dbName") String dbName
    ) {
        Preconditions.check();

        DropEphemeralDbResult result = runWithExceptionLogging(
            "Drop in-memory Cypher database failed",
            () -> {
                GraphCreateEphemeralDatabaseProc.validateNeo4jEnterpriseEdition(databaseService);
                var dbms = GraphDatabaseApiProxy.resolveDependency(databaseService, DatabaseManagementService.class);
                validateDatabaseName(callContext.databaseName(), dbName, dbms);
                var dropMillis = new MutableLong(0);
                try (var ignored = ProgressTimer.start(dropMillis::setValue)) {
                    dbms.dropDatabase(dbName);
                }
                return new DropEphemeralDbResult(dbName, dropMillis.getValue());
            }
        );

        return Stream.of(result);
    }

    @Procedure(name = "gds.alpha.drop.cypherdb", mode = WRITE, deprecatedBy = "gds.ephemeral.database.drop")
    @Description(DESCRIPTION)
    @Internal
    @Deprecated(forRemoval = true)
    public Stream<DropEphemeralDbResult> dropDb(
        @Name(value = "dbName") String dbName
    ) {
        executionContext()
            .metricsFacade()
            .deprecatedProcedures().called("gds.alpha.drop.cypherdb");

        executionContext()
            .log()
            .warn("Procedure `gds.alpha.drop.cypherdb` has been deprecated, please use `gds.ephemeral.database.drop`.");
        
        return dropInMemoryDatabase(dbName);
    }

    private static void validateDatabaseName(String activeDbName, String dbName, DatabaseManagementService dbms) {
        if (activeDbName.equals(dbName)) {
            throw new IllegalArgumentException("Unable drop the currently active database. Please switch to another database first.");
        }

        if (!dbms.listDatabases().contains(dbName)) {
            throw new DatabaseNotFoundException(formatWithLocale("A database with name `%s` does not exist", dbName));
        }

        var graphName = InMemoryDatabaseCreationCatalog.getRegisteredDbCreationGraphName(dbName);
        if (graphName == null) {
            throw new IllegalArgumentException(formatWithLocale(
                "Database with name `%s` is not an in-memory database",
                dbName
            ));
        }
    }

    public static class DropEphemeralDbResult {
        public final String dbName;
        public final long dropMillis;

        public DropEphemeralDbResult(String dbName, long dropMillis) {
            this.dbName = dbName;
            this.dropMillis = dropMillis;
        }
    }
}
