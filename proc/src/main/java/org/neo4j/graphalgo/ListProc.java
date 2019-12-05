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
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;

public class ListProc {

    private static final String QUERY =
            " CALL dbms.procedures() " +
            " YIELD name, signature, description " +
            " WHERE (name starts with 'algo.' OR name starts with 'gds.') AND name <> 'algo.list' AND ($name IS NULL OR name CONTAINS $name) " +
            " RETURN name, signature, description, 'procedure' AS type " +
            " ORDER BY name UNION " +
            " CALL dbms.functions() " +
            " YIELD name, signature, description " +
            " WHERE name starts with 'algo.' AND ($name IS NULL OR name CONTAINS $name) " +
            " RETURN name, signature, description, 'function' AS type " +
            " ORDER BY name";

    @Context
    public GraphDatabaseService db;

    @Procedure("algo.list")
    @Description("CALL algo.list - lists all algorithm procedures, their description and signature")
    public Stream<ListResult> list(@Name(value = "name", defaultValue = "") String name) {
        return db.execute(QUERY, singletonMap("name", name)).stream().map(ListResult::new);
    }

    public static class ListResult {
        public String name;
        public String description;
        public String signature;
        public String type;

        public ListResult(Map<String, Object> row) {
            this.name = (String) row.get("name");
            this.description = (String) row.get("description");
            this.signature = (String) row.get("signature");
            this.type = (String) row.get("type");
        }
    }
}
