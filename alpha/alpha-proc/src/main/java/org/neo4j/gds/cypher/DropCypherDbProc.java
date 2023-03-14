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
import org.neo4j.gds.ProcPreconditions;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.storageengine.InMemoryDatabaseCreationCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.procedure.Mode.WRITE;

public class DropCypherDbProc extends BaseProc {

    private static final String DESCRIPTION = "Drop a database backed by an in-memory graph";

    @Procedure(name = "gds.alpha.drop.cypherdb", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<DropCypherDbResult> dropDb(
        @Name(value = "dbName") String dbName
    ) {
        ProcPreconditions.check();

        DropCypherDbResult result = runWithExceptionLogging(
            "Drop in-memory Cypher database failed",
            () -> {
                GraphCreateCypherDbProc.validateNeo4jEnterpriseEdition(databaseService);
                var dbms = GraphDatabaseApiProxy.resolveDependency(databaseService, DatabaseManagementService.class);
                validateDatabaseName(dbName, dbms);
                var dropMillis = new MutableLong(0);
                try (var ignored = ProgressTimer.start(dropMillis::setValue)) {
                    dbms.dropDatabase(dbName);
                }
                return new DropCypherDbResult(dbName, dropMillis.getValue());
            }
        );

        return Stream.of(result);
    }

    private static void validateDatabaseName(String dbName, DatabaseManagementService dbms) {
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

    public static class DropCypherDbResult {
        public final String dbName;
        public final long dropMillis;

        public DropCypherDbResult(String dbName, long dropMillis) {
            this.dbName = dbName;
            this.dropMillis = dropMillis;
        }
    }
}
