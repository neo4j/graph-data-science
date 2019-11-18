package bad;

import org.neo4j.graphalgo.annotation.Configuration;

import java.util.Map;

@Configuration("InvalidAnnotationCombiationsConfig")
public interface InvalidAnnotationCombinations {

    @Configuration.Key("key")
    @Configuration.Parameter
    int keyAndParamterTogetherIsNotAllowed();
}
