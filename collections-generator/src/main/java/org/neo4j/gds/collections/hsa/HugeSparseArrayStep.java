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
package org.neo4j.gds.collections.hsa;

import org.neo4j.gds.collections.CollectionStep;
import org.neo4j.gds.collections.HugeSparseArray;

import javax.annotation.processing.ProcessingEnvironment;
import java.nio.file.Path;

public final class HugeSparseArrayStep extends CollectionStep<HugeSparseArrayValidation.Spec> {

    private static final Class<HugeSparseArray> HSA_ANNOTATION = HugeSparseArray.class;

    public static HugeSparseArrayStep of(ProcessingEnvironment processingEnv, Path sourcePath) {
        var validation = new HugeSparseArrayValidation(
            processingEnv.getTypeUtils(),
            processingEnv.getElementUtils(),
            processingEnv.getMessager()
        );

        var mainGenerator = new HugeSparseArrayGenerator();
        var testGenerator = new HugeSparseArrayTestGenerator();

        return new HugeSparseArrayStep(processingEnv, sourcePath, validation, mainGenerator, testGenerator);
    }

    private HugeSparseArrayStep(
        ProcessingEnvironment processingEnv,
        Path sourcePath,
        Validation<HugeSparseArrayValidation.Spec> validation,
        Generator<HugeSparseArrayValidation.Spec> mainGenerator,
        Generator<HugeSparseArrayValidation.Spec> testGenerator
    ) {
        super(processingEnv, sourcePath, validation, mainGenerator, testGenerator);
    }

    @Override
    public String annotation() {
        return HSA_ANNOTATION.getCanonicalName();
    }
}
