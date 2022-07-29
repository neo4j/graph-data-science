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
package org.neo4j.gds.core.io.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.io.file.csv.CsvToGraphStoreImporter;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;

class FileToGraphStoreImporterTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldImportProperties(int concurrency) throws URISyntaxException {

        var exporter = new CsvToGraphStoreImporter(concurrency, importPath(), Neo4jProxy.testLog(), EmptyTaskRegistryFactory.INSTANCE);
        var userGraphStore = exporter.run();

        var graphStore = userGraphStore.graphStore();

        assertThat(userGraphStore.userName()).isEqualTo("UserA");

        var expectedGraph = TestSupport.fromGdl(
                                            "  (n0:A {prop1: 21})" +
                                            ", (n1:A {prop1: 42})" +
                                            ", (n2:A {prop1: 23})" +
                                            ", (n3:A {prop1: 24})" +
                                            ", (:A { prop1: 25})" +
                                            ", (:B)" +
                                            ", (:B)" +
                                            ", (:B)" +
                                            ", (:B)" +
                                            ", (:B)" +
                                            ", (n0)-[:REL {weight: 1.5, height: 2.2}]->(n1)-[:REL {weight: 4.0, height: 2.3}]->(n2)-[:REL {weight: 4.2, height: 2.4}]->(n3)" +
                                            ", (n1)-[:REL1]->(n2)-[:REL1]->(n3)"
        );
        var actualGraph = graphStore.getUnion();
        assertGraphEquals(expectedGraph, actualGraph);
    }

    @Test
    void shouldLogProgress() throws URISyntaxException {
        var log = Neo4jProxy.testLog();
        var exporter = new CsvToGraphStoreImporter(1, importPath(), log, EmptyTaskRegistryFactory.INSTANCE);
        exporter.run();

        log.assertContainsMessage(TestLog.INFO, "Csv import :: Start");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import nodes :: Start");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import nodes :: Finished");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships :: Start");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships 20%");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships 40%");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships 60%");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships 80%");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships 100%");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Import relationships :: Finished");
        log.assertContainsMessage(TestLog.INFO, "Csv import :: Finished");
    }

    private Path importPath() throws URISyntaxException {
        var uri = Objects.requireNonNull(getClass().getClassLoader().getResource("CsvToGraphStoreImporterTest")).toURI();
        return Paths.get(uri);
    }

}
