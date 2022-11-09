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
package org.neo4j.gds.spanningtree;

import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.impl.spanningtree.Prim;
import org.neo4j.gds.impl.spanningtree.SpanningTreeAlgorithmFactory;
import org.neo4j.gds.impl.spanningtree.SpanningTreeWriteConfig;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_RELATIONSHIP;

@GdsCallable(name = "gds.alpha.spanningTree.write", description = SpanningTreeProcMin.MIN_DESCRIPTION, executionMode = MUTATE_RELATIONSHIP)
public class SpanningTreeShortenedMinWriteSpec extends SpanningTreeWriteSpec {

    @Override
    public String name() {
        return "SpanningTreeMinWrite";
    }

    @Override
    public SpanningTreeAlgorithmFactory<SpanningTreeWriteConfig> algorithmFactory() {
        return new SpanningTreeAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<SpanningTreeWriteConfig> newConfigFunction() {
        return (__, config) -> SpanningTreeWriteConfig.of(Prim.MIN_OPERATOR, config);

    }


}
