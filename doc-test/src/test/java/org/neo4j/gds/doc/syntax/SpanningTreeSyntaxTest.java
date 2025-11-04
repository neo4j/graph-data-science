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

import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.gds.spanningtree.SpanningTreeStatsConfig;
import org.neo4j.gds.spanningtree.SpanningTreeStreamConfig;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

import java.util.List;

class SpanningTreeSyntaxTest extends SyntaxTestBase {

    @Override
    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(SyntaxMode.STREAM, SpanningTreeStreamConfig.class),
            SyntaxModeMeta.of(SyntaxMode.STATS, SpanningTreeStatsConfig.class),
            SyntaxModeMeta.of(SyntaxMode.WRITE, SpanningTreeWriteConfig.class),
            SyntaxModeMeta.of(SyntaxMode.MUTATE, SpanningTreeMutateConfig.class)
        );
    }

    @Override
    protected String adocFile() {
        return "pages/algorithms/minimum-weight-spanning-tree.adoc";
    }
}
