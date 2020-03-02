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
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.asciidoctor.Asciidoctor.Factory.create;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppendixAProcedureListingTest extends BaseProcTest {

    private static final Path ASCIIDOC_PATH = Paths.get("asciidoc");

    private final Asciidoctor asciidoctor = create();

    @BeforeEach
    void setUp() {
        db = TestDatabaseCreator.createTestDatabase();
        Reflections reflections = new Reflections("org.neo4j.graphalgo",
            new MethodAnnotationsScanner());
        reflections
            .getMethodsAnnotatedWith(Procedure.class)
            .stream()
            .map(Method::getDeclaringClass)
            .collect(Collectors.toSet())
            .forEach(procedureClass -> {
                try {
                    registerProcedures(procedureClass);
                } catch (KernelException e) {
                    e.printStackTrace();
                }
            });

        reflections
            .getMethodsAnnotatedWith(UserFunction.class)
            .stream()
            .map(Method::getDeclaringClass)
            .collect(Collectors.toSet())
            .forEach(functionClass -> {
                try {
                    registerFunctions(functionClass);
                } catch (KernelException e) {
                    e.printStackTrace();
                }
            });
    }

    @Test
    void shouldListAll() {
        AppendixAProcedureListingProcessor procedureListingProcessor = new AppendixAProcedureListingProcessor();
        asciidoctor.javaExtensionRegistry().treeprocessor(procedureListingProcessor);

        File file = ASCIIDOC_PATH.resolve("appendix-a.adoc").toFile();
        assertTrue(file.exists() && file.canRead());
        asciidoctor.loadFile(file, Collections.emptyMap());

        List<String> registeredProcedures = runQuery("CALL gds.list()", result -> {
            List<String> procedures = new ArrayList<>();
            while(result.hasNext()) {
                Map<String, Object> next = result.next();
                String name = next.get("name").toString();
                procedures.add(name);
            }
            return procedures;
        });

        Collection<String> documentedProcedures = procedureListingProcessor.procedures();
        registeredProcedures.add("gds.list");
        registeredProcedures.removeAll(documentedProcedures);
        assertThat(registeredProcedures, is(empty()));
    }

}
