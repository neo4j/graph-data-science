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
package org.neo4j.graphalgo.doc;

import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStatsProc;
import org.neo4j.gds.embeddings.fastrp.FastRPStreamProc;
import org.neo4j.gds.embeddings.fastrp.FastRPWriteProc;
import org.neo4j.graphalgo.beta.fastrp.FastRPExtendedStreamProc;
import org.neo4j.gds.catalog.GraphCreateProc;

import java.util.Arrays;
import java.util.List;

class FastRPDocTest extends DocTestBase {

    @Override
    List<Class<?>> procedures() {
        return Arrays.asList(
            FastRPStreamProc.class,
            FastRPStatsProc.class,
            FastRPMutateProc.class,
            FastRPWriteProc.class,
            FastRPExtendedStreamProc.class,
            GraphCreateProc.class
        );
    }

    @Override
    String adocFile() {
        return "algorithms/fastrp/fastrp.adoc";
    }

}
