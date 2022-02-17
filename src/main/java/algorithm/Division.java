package algorithm;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
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

        /*
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone>");
        List<Tuple> tuples = QueryUtil.retrieveResultsFromQuery((predicates), model).stream().filter(l -> l.get(l.size() - 1).equals("0")).map(t -> {
            String tupleId = t.get(0);
            int classId = Integer.parseInt(t.get(1).split("\\^\\^")[0]);
            int age = Integer.parseInt(t.get(2).split("\\^\\^")[0]);
            String zipcode = t.get(3);
            String religion = t.get(4);
            return new Tuple(tupleId, classId, age, zipcode, religion);
        }).collect(Collectors.toList());
         */
        return tuples.stream().collect(Collectors.groupingBy(Tuple::getClassId));
    }




    public static Map<Set<String>, List<Integer>> createMapSignatures(Map<Integer, List<Tuple>> mapEqClassesAllTuples, Set<Integer> aliveEqClasses) {
        /*Map<Set<String>, Integer> mapSignatures =
                oldTuples.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getValue().stream().map(Tuple::getReligion).collect(Collectors.toSet()), Map.Entry::getKey));
*/

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


    public static void main(String[] args) {

        Model model = ModelFactory.createDefaultModel();
        model.read("src/main/resources/graph.trig");
        List<String> predicates = new ArrayList<>();
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID>");
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#age>");
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode>");
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#disease>");
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone>");


        List<Tuple> tuples = Division.retrieveAllTuples(predicates, model);
        System.out.println("TUPLES SIZE : " + tuples.size());
        List<Tuple> aliveTuples = Division.filterAliveTuples(tuples);
        System.out.println("ALIVE TUPLES SIZE : " + aliveTuples.size());


        Map<Integer, List<Tuple>> eqClassesAlive = Division.createMapEquivalenceClasses(new ArrayList<>());
        Map<Integer, List<Tuple>> eqClassesAll = Division.createMapEquivalenceClasses(new ArrayList<>());

        System.out.println("AAA");

        Map<Set<String>, List<Integer>> mapSignatures = Division.createMapSignatures(eqClassesAll, eqClassesAlive.keySet());
        mapSignatures.forEach((k,v) -> {
            System.out.println("Signature " + k + " ==>  " + v);
        });

        System.out.println("SIGN SIZE : " + mapSignatures.size());

        Map<Set<String>, Bucket> buckets = Division.createBuckets(eqClassesAlive, mapSignatures);
        System.out.println("BUCK SIZE : " + buckets.size());



        Tuple t1 = new Tuple("Emily", -1, 25, "21000", "Flu", false, false);
        Tuple t2 = new Tuple("Mary", -1, 46, "30000", "Gastro", false, false);
        Tuple t3 = new Tuple("Ray", -1, 54, "31000", "Dyspepsia", false, false);
        Tuple t4 = new Tuple("Tom", -1, 60, "44000", "Gastro", false, false);
        Tuple t5 = new Tuple("Vince", -1, 65, "36000", "Flu", false, false);
        List<Tuple> newTuples = new ArrayList<>();
        newTuples.add(t1);
        newTuples.add(t2);
        newTuples.add(t3);
        newTuples.add(t4);
        newTuples.add(t5);

        int m = 2;


        Map<String, List<Tuple>> mapNewTuples = Balancing.groupTuplesBySa(newTuples);
        int indexFakeCounter = 0;
        for (Map.Entry<Set<String>, Bucket> setBucketEntry : buckets.entrySet()) {
            Bucket b = setBucketEntry.getValue();
            indexFakeCounter = b.balance(mapNewTuples, m, indexFakeCounter);
        }

        buckets.forEach((k,v) -> {
            System.out.println("Bucket 1" + k + " ==>  ");
            System.out.println(v);
            System.out.println("\n");
        });

        System.out.println("XXXXXXXXX\n");

        Map<String, Integer> mapSaSize = Balancing.computeMapSaSize(mapNewTuples);
        mapSaSize.forEach((k,v) -> System.out.println(k + " -> " + v));
        Assignment.assignment(m, mapNewTuples, mapSaSize, buckets);

        buckets.forEach((k,v) -> {
            System.out.println("Bucket 2 " + k + " ==>  ");
            System.out.println(v);
            System.out.println("\n");
        });


        List<Bucket> finalBuckets = Split.split(buckets, 80, 25000);

        finalBuckets.forEach( x -> {
            System.out.println("Bucket 3 " + x.getSignature() + " ==>  ");
            System.out.println(x);
            System.out.println("\n");
        });

        System.out.println("FIN");



    }


}
