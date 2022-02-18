package algorithm;

import org.apache.jena.rdf.model.Model;
import query.QueryUtil;

import java.util.*;
import java.util.stream.Collectors;

public class Division {


    public static List<Tuple> retrieveAllTuples(List<String> predicates, Model model) {
        List<Tuple> tuples = QueryUtil.retrieveResultsFromQuery((predicates), model).stream().map(t -> {
            String tupleId = t.get(0);
            int classId = Integer.parseInt(t.get(1).split("\\^\\^")[0]);
            int age = Integer.parseInt(t.get(2).split("\\^\\^")[0]);
            String zipcode = t.get(3);
            String religion = t.get(4);
            boolean isDead;
            if (t.get(5).equals("0")) {
                isDead = false;
            }
            else {
                isDead = true;
            }
            return new Tuple(tupleId, classId, age, zipcode, religion, isDead, false);
        }).collect(Collectors.toList());

        return tuples;
    }


    public static List<Tuple> filterOldTuples(List<Tuple> tuples) {
        return tuples.stream().filter(t -> t.getClassId() >= 0).collect(Collectors.toList());
    }

    public static List<Tuple> filterNewTuples(List<Tuple> tuples) {
        return tuples.stream().filter(t -> t.getClassId() < 0).collect(Collectors.toList());
    }


    public static List<Tuple> filterAliveTuples(List<Tuple> tuples) {
        return tuples.stream().filter(t -> !t.getIsDead()).collect(Collectors.toList());
    }

    public static List<Tuple> filterDeadTuples(List<Tuple> tuples) {
        return tuples.stream().filter(Tuple::getIsDead).collect(Collectors.toList());
    }


    public static Map<Integer, List<Tuple>> createMapEquivalenceClasses(List<Tuple> tuples) {
        return tuples.stream().collect(Collectors.groupingBy(Tuple::getClassId));
    }




    public static Map<Set<String>, List<Integer>> createMapSignatures(Map<Integer, List<Tuple>> mapEqClassesAllTuples, Set<Integer> aliveEqClasses) {
        Map<Set<String>, List<Integer>> mapSignatures =
                mapEqClassesAllTuples.entrySet().stream().filter(e -> aliveEqClasses.contains(e.getKey()))
                        .map(e -> {
                            Set<String> signature = e.getValue().stream().map(Tuple::getSa).collect(Collectors.toSet());
                            return new AbstractMap.SimpleEntry<>(e.getKey(), signature);
                        }).collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        return mapSignatures;
    }



    public static Map<Set<String> ,Bucket> createBuckets(Map<Integer, List<Tuple>> equivalenceClasses, Map<Set<String>, List<Integer>> signatures) {
        Map<Set<String>, Bucket> buckets = new HashMap<>();

        signatures.forEach((signature, ec) -> {
            Bucket b = new Bucket(signature);
            ec.forEach(x -> {
                b.addTuples(equivalenceClasses.get(x));
            });
            buckets.put(signature, b);
        });

        return buckets;
    }


}
