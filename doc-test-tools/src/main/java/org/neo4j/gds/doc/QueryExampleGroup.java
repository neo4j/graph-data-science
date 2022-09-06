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
package org.neo4j.gds.doc;

import org.neo4j.gds.annotation.ValueClass;

import java.util.List;

/**
 * Query examples are grouped by displayName like so ('myGroup' becomes the displayName):
 *
 * <pre>
 * [role=query-example, group=myGroup]
 * --
 * [...]
 * --
 *
 * [role=query-example, group=myGroup]
 * --
 * [...]
 * --
 * </pre>
 *
 * When it comes to executing tests, the query examples in a group are executed in document order with a single for-each
 * query. This ensures the effects from one query are visible to other queries. Think mutate followed by stream, or
 * write followed by arbitrary Cypher.
 *
 * If you do not specify the group attribute, your query is assigned a dummy displayName and executed in isolation.
 */
@ValueClass
public interface QueryExampleGroup {
    /**
     * Only used for test names in JUnit
     */
    String displayName();

    List<QueryExample> queryExamples();

    static ImmutableQueryExampleGroup.Builder builder() {
        return ImmutableQueryExampleGroup.builder();
    }
}
