/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.nodesim.NodeSimilarityStreamProc;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryConsumingTreeProcessorTest extends DocTestBase {

    @Override
    List<Class<?>> procedures() {
        return Collections.singletonList(NodeSimilarityStreamProc.class);
    }

    @Override
    String adocFile() {
        return "treeprocessor.adoc";
    }

    @Test
    void runTest() {
        asciidoctor.javaExtensionRegistry().treeprocessor(defaultTreeProcessor());
        try {
            File file = Paths.get(getClass().getClassLoader().getResource("treeprocessor.adoc").toURI()).toFile();
            assertTrue(file.exists() && file.canRead());
            asciidoctor.loadFile(file, Collections.emptyMap());
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

}
