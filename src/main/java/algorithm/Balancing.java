package algorithm;


import java.util.*;
import java.util.stream.Collectors;

public class Balancing {

    public static Map<String, List<Tuple>> groupTuplesBySa(List<Tuple> tuples) {
        return tuples.stream().collect(Collectors.groupingBy(Tuple::getSa));
    }


    public static boolean mEligibilityTest(int mostRepresentedAttributeCount, int totalCount, int m) {
        return (mostRepresentedAttributeCount * 1.0 / totalCount) <= (1.0 / m);
    }


    public static Map<String, Integer> computeMapSaSize(Map<String, List<Tuple>> mapTuples) {
        Map<String, Integer> m = mapTuples.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
        return m;
    }


    public static AbstractMap.SimpleEntry<String, Integer> findMostRepresentedAttribute(Map<String, Integer> mapAttributeSize) {
        Map.Entry<String, Integer> entry = mapAttributeSize.entrySet().stream().max(Map.Entry.comparingByValue()).get();
        return new AbstractMap.SimpleEntry<>(entry.getKey(), entry.getValue());
    }

}