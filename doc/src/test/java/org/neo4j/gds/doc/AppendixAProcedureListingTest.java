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
package org.neo4j.gds.doc;

import org.asciidoctor.Asciidoctor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AppendixAProcedureListingTest extends BaseProcTest {

    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");

    private final Asciidoctor asciidoctor = create();
    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.graphalgo",
        "org.neo4j.gds"
    );

    @BeforeEach
    void setUp() {
        PACKAGES_TO_SCAN.stream()
            .map(this::createReflections)
            .forEach(reflections -> {
                registerProcedures(reflections);
                registerFunctions(reflections);
            });
    }

    @Test
    void countShouldMatch() {
        var registeredProcedures = new LinkedList<>();
        runQueryWithRowConsumer("CALL gds.list() YIELD name", row -> {
            registeredProcedures.add(row.getString("name"));
        });
        registeredProcedures.add("gds.list");

        // If you find yourself updating this count, please also update the count in SmokeTest.kt
        int expectedCount = 286;
        assertEquals(
            expectedCount,
            registeredProcedures.size(),
            "The expected and registered procedures don't match. Please also update the SmokeTest counts."
        );
    }

    @Test
    void shouldListAll() {
        var procedureListingProcessor = new AppendixAProcedureListingProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(procedureListingProcessor);

        var baseDirectory =  ASCIIDOC_PATH.resolve("operations-reference");
        var files = baseDirectory.toFile().listFiles();

        assertThat(files)
            .isNotNull()
            .allSatisfy(file -> assertThat(file).exists().canRead());

        Arrays.stream(files).forEach(file -> asciidoctor.loadFile(file, Collections.emptyMap()));

        var registeredProcedures = new ArrayList<String>();
        runQueryWithRowConsumer("CALL gds.list() YIELD name", row -> {
            registeredProcedures.add(row.getString("name"));
        });
        registeredProcedures.add("gds.list");

        List<String> documentedProcedures = procedureListingProcessor.procedures();

        Set<String> undocumentedProcs = new HashSet<>(registeredProcedures);
        documentedProcedures.forEach(undocumentedProcs::remove);

        Set<String> nonExistingProcs = new HashSet<>(documentedProcedures);
        registeredProcedures.forEach(nonExistingProcs::remove);

        assertThat(registeredProcedures)
            .withFailMessage(
                "Undocumented procedures: " + undocumentedProcs + System.lineSeparator()
                + "Non existing procedures: " + nonExistingProcs)
            .containsExactlyInAnyOrderElementsOf(documentedProcedures);
    }

    private Reflections createReflections(String pkg) {
        return new Reflections(
            pkg,
            new MethodAnnotationsScanner()
        );
    }

    private void registerProcedures(Reflections reflections) {
        reflections
            .getMethodsAnnotatedWith(Procedure.class)
            .stream()
            .map(Method::getDeclaringClass)
            .distinct()
            .forEach(procedureClass -> {
                try {
                    registerProcedures(procedureClass);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
    }

    private void registerFunctions(Reflections reflections) {
        reflections
            .getMethodsAnnotatedWith(UserFunction.class)
            .stream()
            .map(Method::getDeclaringClass)
            .distinct()
            .forEach(functionClass -> {
                try {
                    registerFunctions(functionClass);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
    }

}
