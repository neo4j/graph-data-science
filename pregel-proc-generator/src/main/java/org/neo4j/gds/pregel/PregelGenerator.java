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
package org.neo4j.gds.pregel;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.TypeSpec;
import org.neo4j.gds.pregel.generator.AlgorithmFactoryGenerator;
import org.neo4j.gds.pregel.generator.AlgorithmGenerator;
import org.neo4j.gds.pregel.generator.ProcedureGenerator;
import org.neo4j.gds.pregel.generator.SpecificationGenerator;
import org.neo4j.gds.pregel.generator.TypeNames;

import javax.lang.model.element.Element;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The PregelGenerator's job is to coordinate the generation of code necessary to use a Pregel computation via Neo4j procedures.
 */
class PregelGenerator {

    // produces @Generated meta info
    private final Optional<AnnotationSpec> generatedAnnotationSpec;

    PregelGenerator(Optional<AnnotationSpec> generatedAnnotationSpec) {
        this.generatedAnnotationSpec = generatedAnnotationSpec;
    }

    Stream<TypeSpec> generate(PregelValidation.Spec pregelSpec) {
        var typeNames = new TypeNames(
            pregelSpec.rootPackage(),
            pregelSpec.computationName(),
            pregelSpec.configTypeName()
        );
        var originatingElement = pregelSpec.element();

        var typeSpecs = new ArrayList<TypeSpec>();

        var algorithmSpec = new AlgorithmGenerator(typeNames).generate(generatedAnnotationSpec);
        typeSpecs.add(withOriginatingElement(algorithmSpec, originatingElement));

        var algorithmFactorySpec = new AlgorithmFactoryGenerator(typeNames).generate(generatedAnnotationSpec);
        typeSpecs.add(withOriginatingElement(algorithmFactorySpec, originatingElement));

        var specificationGenerator = new SpecificationGenerator(typeNames);
        var procedureGenerator = new ProcedureGenerator(typeNames, pregelSpec.procedureName(), pregelSpec.description());

        var requiresInverseIndex = pregelSpec.requiresInverseIndex();
        for (var mode : pregelSpec.procedureModes()) {
            var procedure = procedureGenerator.generate(mode, requiresInverseIndex, generatedAnnotationSpec);
            typeSpecs.add(withOriginatingElement(procedure, originatingElement));

            var specification = specificationGenerator.generate(mode, generatedAnnotationSpec);
            typeSpecs.add(withOriginatingElement(specification, originatingElement));
        }
        return typeSpecs.stream();
    }

    private TypeSpec withOriginatingElement(TypeSpec typeSpec, Element element) {
        return typeSpec.toBuilder().addOriginatingElement(element).build();
    }
}
