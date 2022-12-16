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
package org.neo4j.gds.doc.syntax;

import java.util.List;

class GraphCatalogRelationshipOperationsSyntaxTest extends SyntaxTestBase {

    @Override
    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(SyntaxMode.STREAM_SINGLE_PROPERTY),
            SyntaxModeMeta.of(SyntaxMode.STREAM_TOPOLOGY),
            SyntaxModeMeta.of(SyntaxMode.STREAM_MULTIPLE_PROPERTIES),
            SyntaxModeMeta.of(SyntaxMode.CONVERT_TO_UNDIRECTED),
            SyntaxModeMeta.of(SyntaxMode.WRITE),
            SyntaxModeMeta.of(SyntaxMode.DELETE_RELATIONSHIPS)
        );
    }

    @Override
    protected String adocFile() {
        return "pages/graph-catalog-relationship-ops.adoc";
    }
}
