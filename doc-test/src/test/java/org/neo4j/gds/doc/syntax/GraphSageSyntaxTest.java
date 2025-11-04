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

import org.neo4j.gds.embeddings.graphsage.algo.GraphSageMutateConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageWriteConfig;

import java.util.List;

class GraphSageSyntaxTest extends SyntaxTestBase {

    @Override
    protected Iterable<SyntaxModeMeta> syntaxModes() {
        return List.of(
            SyntaxModeMeta.of(SyntaxMode.STREAM, GraphSageStreamConfig.class),
            SyntaxModeMeta.of(SyntaxMode.TRAIN, GraphSageTrainConfig.class),
            SyntaxModeMeta.of(SyntaxMode.MUTATE, GraphSageMutateConfig.class),
            SyntaxModeMeta.of(SyntaxMode.WRITE, GraphSageWriteConfig.class)
        );
    }

    @Override
    protected String adocFile() {
        return "pages/machine-learning/node-embeddings/graph-sage.adoc";
    }
}
