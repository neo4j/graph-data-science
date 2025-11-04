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

import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainStatsConfig;
import org.neo4j.gds.louvain.LouvainStreamConfig;
import org.neo4j.gds.louvain.LouvainWriteConfig;

import java.util.List;

import static org.neo4j.gds.doc.syntax.SyntaxMode.MUTATE;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STATS;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STREAM;
import static org.neo4j.gds.doc.syntax.SyntaxMode.WRITE;

class LouvainSyntaxTest extends SyntaxTestBase {

    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(STATS, LouvainStatsConfig.class),
            SyntaxModeMeta.of(STREAM, LouvainStreamConfig.class),
            SyntaxModeMeta.of(MUTATE, LouvainMutateConfig.class),
            SyntaxModeMeta.of(WRITE, LouvainWriteConfig.class)
        );
    }

    @Override
    protected String adocFile() {
        return "pages/algorithms/louvain.adoc";
    }
}
