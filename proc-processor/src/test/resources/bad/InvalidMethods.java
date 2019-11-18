package bad;

import org.neo4j.graphalgo.annotation.Configuration;

import java.util.Map;

@Configuration("InvalidMethodsConfig")
public interface InvalidMethods {

    char charIsNotSupported();

    void voidIsNotSupported();

    int[] arraysAreNotSupported();

    String parametersAreNotSupported(boolean nope);

    <A> A genericsAreNotSupported();

    <V> Map<String, V> genericsAreNotSupported2();

    int throwsDeclarationsAreNotSupported() throws Exception;

    String throwsDeclarationsAreNotSupported2() throws IllegalArgumentException;
}
