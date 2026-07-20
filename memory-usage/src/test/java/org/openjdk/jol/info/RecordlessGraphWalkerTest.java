/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.openjdk.jol.info;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.openjdk.jol.vm.VM;

import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordlessGraphWalkerTest {

    @Nested
    class SizeOfObjectTests {
        @Test
        void getSizeOfClass() {
            var current = VM.current();
            var clazz = new MyClass();

            assertThatNoException().isThrownBy(() -> current.sizeOf(clazz));
        }

        @Test
        void getSizeOfClassWithRecordField() {
            var current = VM.current();
            var clazz = new Inner(new MyRecord("foobar"));

            assertThatNoException().isThrownBy(() -> current.sizeOf(clazz));
        }

        @Test
        void gettingSizeOfRecordIsNotSupported() {
            var current = VM.current();
            var record = new MyRecord("foobar");

            assertThatThrownBy(() -> current.sizeOf(record))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot get the field offset");
        }
    }

    @Nested
    @ParameterizedClass
    @MethodSource("visitors")
    class WalkAndVisitTests {
        @Parameter(0)
        String visitorType;

        @Parameter(1)
        GraphVisitor visitor;

        static Stream<Arguments> visitors() {
            return Stream.of(
                Arguments.of("no-op", (GraphVisitor) gpr -> {}),
                // Some simple visitor that reads the size, similar to what we do in production
                Arguments.of("size", (GraphVisitor) GraphPathRecord::size)
            );
        }

        @Test
        void walkClass() {
            var clazz = new MyClass();
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            var result1 = jolGraphWalker.walk(clazz);
            var result2 = recordlessGraphWalker.walk(clazz);

            assertThat(result1.getClasses()).containsExactlyInAnyOrderElementsOf(result2.getClasses());
        }

        @Test
        void walkArray() {
            var array = new MyClass[]{new MyClass()};
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            var result1 = jolGraphWalker.walk(array);
            var result2 = recordlessGraphWalker.walk(array);

            assertThat(result1.getClasses()).containsExactlyInAnyOrderElementsOf(result2.getClasses());
        }

        @Test
        void walkClassWithArray() {
            var array = new MyArray(new Object[]{new MyClass()});
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            var result1 = jolGraphWalker.walk(array);
            var result2 = recordlessGraphWalker.walk(array);

            assertThat(result1.getClasses()).containsExactlyInAnyOrderElementsOf(result2.getClasses());
        }

        @Test
        void walkRecord() {
            var clazz = new MyRecord("foo");
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            assertThatThrownBy(() -> jolGraphWalker.walk(clazz))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot get the field offset");
            var result = recordlessGraphWalker.walk(clazz);

            var expected = Set.of(MyRecord.class, String.class, byte[].class);
            assertThat(result.getClasses()).containsExactlyInAnyOrderElementsOf(expected);
        }

        @Test
        void walkClassWithRecordField() {
            var clazz = new Inner(new MyRecord("foobar"));
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            assertThatThrownBy(() -> jolGraphWalker.walk(clazz))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot get the field offset");
            var result = recordlessGraphWalker.walk(clazz);

            var expected = Set.of(Inner.class, MyRecord.class, String.class, byte[].class);
            assertThat(result.getClasses()).containsExactlyInAnyOrderElementsOf(expected);
        }

        @Test
        void walkArrayWithRecord() {
            var array = new MyRecord[]{ new MyRecord("foobar") };
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            assertThatThrownBy(() -> jolGraphWalker.walk(array))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot get the field offset");
            var result = recordlessGraphWalker.walk(array);

            var expected = Set.of(MyRecord.class, String.class, byte[].class);
            assertThat(result.getClasses()).containsExactlyInAnyOrderElementsOf(expected);
        }

        @Test
        void walkClassWithArrayWithRecord() {
            var array = new MyArray(new Object[]{ new MyRecord("foobar") });
            var jolGraphWalker = new GraphWalker(visitor);
            var recordlessGraphWalker = new RecordlessGraphWalker(visitor);

            assertThatThrownBy(() -> jolGraphWalker.walk(array))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot get the field offset");
            var result = recordlessGraphWalker.walk(array);

            var expected = Set.of(MyArray.class, Object[].class, MyRecord.class, String.class, byte[].class);
            assertThat(result.getClasses()).containsExactlyInAnyOrderElementsOf(expected);
        }
    }


    record MyRecord(
        // We need to have at least one field to trigger jol to fail.
        String s
    ) {}

    private static class MyClass {
        private final String s;

        private MyClass() {
            this.s = "outer";
        }
    }

    private static class MyArray {
        private final Object[] array;

        private MyArray(Object[] array) {
            this.array = array;
        }
    }

    private static class Inner {
        // We need to have an additional field to trigger jol to fail.
        private String s;
        private MyRecord myRecord;

        Inner(MyRecord r) {
            this.s = "baz";
            this.myRecord = r;
        }
    }
}
