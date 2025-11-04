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

import org.neo4j.gds.hdbscan.HDBScanMutateConfig;
import org.neo4j.gds.hdbscan.HDBScanStatsConfig;
import org.neo4j.gds.hdbscan.HDBScanStreamConfig;
import org.neo4j.gds.hdbscan.HDBScanWriteConfig;

import java.util.List;

import static org.neo4j.gds.doc.syntax.SyntaxMode.MUTATE;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STATS;
import static org.neo4j.gds.doc.syntax.SyntaxMode.STREAM;
import static org.neo4j.gds.doc.syntax.SyntaxMode.WRITE;

class HDBScanSyntaxTest extends SyntaxTestBase {

    @Override
    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(STREAM, HDBScanStreamConfig.class),
            SyntaxModeMeta.of(STATS, HDBScanStatsConfig.class),
            SyntaxModeMeta.of(MUTATE, HDBScanMutateConfig.class),
            SyntaxModeMeta.of(WRITE, HDBScanWriteConfig.class)
        );
    }

    @Override
    protected String adocFile() {
        return "pages/algorithms/hdbscan.adoc";
    }
}
