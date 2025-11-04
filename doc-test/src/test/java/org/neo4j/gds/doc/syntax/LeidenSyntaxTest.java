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

import org.neo4j.gds.leiden.LeidenMutateConfig;
import org.neo4j.gds.leiden.LeidenStatsConfig;
import org.neo4j.gds.leiden.LeidenStreamConfig;
import org.neo4j.gds.leiden.LeidenWriteConfig;

import java.util.List;

import static org.neo4j.gds.doc.syntax.SyntaxMode.MUTATE;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STATS;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STREAM;
import static org.neo4j.gds.doc.syntax.SyntaxMode.WRITE;

class LeidenSyntaxTest extends SyntaxTestBase {


    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(STATS, LeidenStatsConfig.class),
            SyntaxModeMeta.of(STREAM, LeidenStreamConfig.class),
            SyntaxModeMeta.of(MUTATE, LeidenMutateConfig.class),
            SyntaxModeMeta.of(WRITE, LeidenWriteConfig.class)
        );
    }
    @Override
    protected String adocFile() {
        return "pages/algorithms/leiden.adoc";
    }
}
