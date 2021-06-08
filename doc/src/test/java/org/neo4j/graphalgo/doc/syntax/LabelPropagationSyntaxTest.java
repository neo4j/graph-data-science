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
package org.neo4j.graphalgo.doc.syntax;

import org.neo4j.graphalgo.labelpropagation.LabelPropagationMutateProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStatsProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationStreamProc;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationWriteProc;

import java.util.Map;

import static org.neo4j.graphalgo.doc.syntax.ProcedureSyntaxChecker.SyntaxMode.MUTATE;
import static org.neo4j.graphalgo.doc.syntax.ProcedureSyntaxChecker.SyntaxMode.STATS;
import static org.neo4j.graphalgo.doc.syntax.ProcedureSyntaxChecker.SyntaxMode.STREAM;
import static org.neo4j.graphalgo.doc.syntax.ProcedureSyntaxChecker.SyntaxMode.WRITE;

class LabelPropagationSyntaxTest extends SyntaxTestBase {

    @Override
    protected Map<ProcedureSyntaxChecker.SyntaxMode, Class<?>> syntaxModes() {
        return Map.of(
            STREAM, LabelPropagationStreamProc.StreamResult.class,
            MUTATE, LabelPropagationMutateProc.MutateResult.class,
            STATS, LabelPropagationStatsProc.StatsResult.class,
            WRITE, LabelPropagationWriteProc.WriteResult.class
        );
    }

    @Override
    String adocFile() {
        return "algorithms/label-propagation/label-propagation.adoc";
    }
}
