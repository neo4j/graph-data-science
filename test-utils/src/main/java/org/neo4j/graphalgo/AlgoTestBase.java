/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

public class AlgoTestBase {

    public GraphDatabaseAPI db;

    protected void runQuery(String query) {
        runQuery(db, query);
    }

    protected void runQuery(String query, Consumer<Result.ResultRow> check) {
        runQuery(db, query, check);
    }

    protected void runQuery(GraphDatabaseService databaseAPI, String query) {
        QueryRunner.runInTransaction(databaseAPI, () -> databaseAPI.execute(query).close());
    }

    protected void runQuery(GraphDatabaseService db, String query, Consumer<Result.ResultRow> check) {
        QueryRunner.runQueryWithRowConsumer(db, query, check);
    }
}
