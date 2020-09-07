/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.asciidoctor.Asciidoctor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppendixAProcedureListingTest extends BaseProcTest {

    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");

    private final Asciidoctor asciidoctor = create();
    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.graphalgo",
        "org.neo4j.gds.embeddings"
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

        int expectedCount = 179;
        assertEquals(
            expectedCount,
            registeredProcedures.size(),
            "The expected and registered procedures don't match. Please also update the SmokeTest counts."
        );
    }

    @Test
    void shouldListAll() {
        AppendixAProcedureListingProcessor procedureListingProcessor = new AppendixAProcedureListingProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(procedureListingProcessor);

        File file = ASCIIDOC_PATH.resolve("appendix-a.adoc").toFile();
        assertTrue(file.exists() && file.canRead());
        asciidoctor.loadFile(file, Collections.emptyMap());

        List<String> registeredProcedures = new LinkedList<>();
        runQueryWithRowConsumer("CALL gds.list() YIELD name", row -> {
            registeredProcedures.add(row.getString("name"));
        });

        List<String> documentedProcedures = procedureListingProcessor.procedures();
        registeredProcedures.add("gds.list");

        List<String> registeredProceduresCopy = new ArrayList<>(registeredProcedures);
        registeredProceduresCopy.removeAll(documentedProcedures);
        assertThat(registeredProceduresCopy, is(empty()));

        List<String> documentedProceduresCopy = new ArrayList<>(documentedProcedures);
        documentedProceduresCopy.removeAll(registeredProcedures);
        assertThat(documentedProceduresCopy, is(empty()));

        registeredProcedures.sort(String::compareTo);
        documentedProcedures.sort(String::compareTo);
        assertEquals(registeredProcedures, documentedProcedures);

        assertEquals(registeredProcedures.size(), documentedProcedures.size());
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
