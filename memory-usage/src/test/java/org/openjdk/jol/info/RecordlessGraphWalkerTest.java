/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.openjdk.jol.info;

import org.junit.jupiter.api.Test;
import org.openjdk.jol.vm.VM;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordlessGraphWalkerTest {

    record MyRecord(
        // We need to have at least one field to trigger jol to fail.
        String s
    ) {
    }

    private static class Outer {
        private final String s;
        private final Inner inner;

        private Outer(Inner inner) {
            this.s = "outer";
            this.inner = inner;
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

    @Test
    void sizeOfClassSucceeds() {
        var current = VM.current();
        var clazz = new Inner(new MyRecord("foobar"));

        assertThatNoException().isThrownBy(() -> current.sizeOf(clazz));
    }

    @Test
    void sizeOfRecordFails() {
        var current = VM.current();
        var record = new MyRecord("foobar");

        assertThatThrownBy(() -> current.sizeOf(record))
            .isInstanceOf(RuntimeException.class)
            .hasRootCauseInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot get the field offset");
    }

    @Test
    void graphWalkerOfClassFails() {
        var record = new MyRecord("foobar");
        var clazz = new Inner(record);

        assertThatThrownBy(() -> {
            new GraphWalker().walk(clazz);
        })
            .isInstanceOf(RuntimeException.class)
            .hasRootCauseInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Cannot get the field offset");
    }

    @Test
    void walkRecordSucceeds() {
        var record = new MyRecord("foobar");

        assertThatNoException().isThrownBy(() -> new RecordlessGraphWalker().walk(record));
    }

    @Test
    void walkClassSucceeds() {
        var record = new MyRecord("foobar");
        var clazz = new Inner(record);

        assertThatNoException().isThrownBy(() -> new RecordlessGraphWalker().walk(clazz));
    }

    @Test
    void walkClassWithVisitorSucceeds() {
        var record = new MyRecord("foobar");
        var clazz = new Inner(record);

        assertThatNoException().isThrownBy(() -> new RecordlessGraphWalker(GraphPathRecord::size).walk(clazz));
    }

    @Test
    void walkNestedClassWithVisitorSucceeds() {
        var record = new MyRecord("foobar");
        var inner = new Inner(record);
        var outer = new Outer(inner);

        assertThatNoException().isThrownBy(() -> new RecordlessGraphWalker(GraphPathRecord::size).walk(outer));
    }
}