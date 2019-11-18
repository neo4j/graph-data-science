package bad;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("InvalidConversionsClassesConfig")
public interface InvalidConversionsClasses {

    @Configuration.ConvertWith("")
    int emptyConverter();

    @Configuration.ConvertWith("multipleOverloads")
    int multi();

    static int multipleOverloads(String input) {
        return 42;
    }

    static int multipleOverloads(long input) {
        return 42;
    }

    @Configuration.ConvertWith("bad.class.does.not.exist#foo")
    int classDoesNotExist();

    @Configuration.ConvertWith("methodDoesNotExist")
    int converterMethodDoesNotExist();

    @Configuration.ConvertWith("bad.InvalidConversionsClasses#methodDoesNotExist")
    int fullQualifiedConverterMethodDoesNotExist();

    @Configuration.ConvertWith("bad.InvalidConversionsClasses#")
    int missingMethodName();
}
