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

import org.neo4j.gds.hits.HitsConfig;

import java.util.List;
import java.util.Set;

public class HitsSyntaxTest extends SyntaxTestBase {

    @Override
    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(SyntaxMode.STREAM, HitsConfig.class, Set.of(
                "writeConcurrency",
                "writeProperty",
                "mutateProperty"
            )),
            SyntaxModeMeta.of(SyntaxMode.STATS, HitsConfig.class, Set.of(
                "writeConcurrency",
                "writeProperty",
                "mutateProperty"
            )),
            SyntaxModeMeta.of(SyntaxMode.MUTATE, HitsConfig.class, Set.of(
                "writeConcurrency",
                "writeProperty"
            )),
            SyntaxModeMeta.of(SyntaxMode.WRITE, HitsConfig.class, Set.of(
                "mutateProperty"
            ))
        );
    }

    @Override
    protected Set<String> ignoredParameters() {
        return Set.of(
            "sudo",
            "username",
            "writeToResultStore",
            "relationshipWeightProperty"
        );
    }

    @Override
    protected String adocFile() {
        return "pages/algorithms/hits.adoc";
    }

}
