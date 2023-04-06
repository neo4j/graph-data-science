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
package org.neo4j.gds.collections;

import com.google.auto.common.BasicAnnotationProcessor;
import com.google.auto.service.AutoService;
import org.neo4j.gds.collections.haa.HugeAtomicArrayStep;
import org.neo4j.gds.collections.hsa.HugeSparseArrayStep;
import org.neo4j.gds.collections.hsl.HugeSparseListStep;

import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.Processor;
import javax.lang.model.SourceVersion;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@AutoService(Processor.class)
public class CollectionsProcessor extends BasicAnnotationProcessor {

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_11;
    }

    @Override
    protected Iterable<? extends Step> steps() {
        var sourcePath = fetchSourcePath(processingEnv.getFiler(), processingEnv.getMessager());

        return List.of(
            HugeSparseArrayStep.of(processingEnv, sourcePath),
            HugeSparseListStep.of(processingEnv, sourcePath),
            HugeAtomicArrayStep.of(processingEnv, sourcePath)
        );
    }

    private Path fetchSourcePath(Filer filer, Messager messager) {
        JavaFileObject tmpFile = null;
        try {
            // We want to retrieve the path for generated source files managed
            // by the filer object and its underlying FileManager. In order to
            // retrieve it, we create a temporary file, convert it into a Path
            // object and navigate to the parent directories.
            tmpFile = filer.createSourceFile("tmpFile");
            // the new file is open for writing; we don't do that so we close it
            tmpFile.openWriter().close();
            // build/generated/sources/annotationProcessor/java/main/tmpFile
            var tmpFilePath = Path.of(tmpFile.toUri());
            // build/generated/sources/annotationProcessor/java/main
            var mainPath = tmpFilePath.getParent();
            // build/generated/sources/annotationProcessor/java
            return mainPath.getParent();
        } catch (IOException e) {
            messager.printMessage(Diagnostic.Kind.ERROR, "Unable to determine source path");
        } finally {
            if (tmpFile != null) {
                tmpFile.delete();
            }
        }

        return null;
    }

}
