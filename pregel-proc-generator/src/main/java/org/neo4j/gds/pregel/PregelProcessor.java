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

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.common.GeneratedAnnotationSpecs;
import com.google.auto.service.AutoService;

import javax.annotation.processing.Processor;
import javax.lang.model.SourceVersion;
import java.util.Set;

@AutoService(Processor.class)
public final class PregelProcessor extends BasicAnnotationProcessor {

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_17;
    }

    @Override
    protected Iterable<? extends Step> steps() {
        var pregelValidation = new PregelValidation(
            processingEnv.getMessager(),
            processingEnv.getElementUtils(),
            processingEnv.getTypeUtils()
        );

        var generatedAnnotationSpec = GeneratedAnnotationSpecs.generatedAnnotationSpec(
            processingEnv.getElementUtils(),
            getSupportedSourceVersion(),
            PregelProcessor.class
        );
        var pregelGenerator = new PregelGenerator(generatedAnnotationSpec);

        var processingStep = new PregelProcessorStep(
            processingEnv.getMessager(),
            processingEnv.getFiler(),
            pregelValidation,
            pregelGenerator
        );

        return Set.of(processingStep);
    }
}
