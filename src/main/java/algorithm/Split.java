package algorithm;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import query.QueryUtil;
import util.Counter;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.stream.Collectors;

public class Split {

    private static class MinMaxAgeZip {
        private int minAge;
        private int maxAge;
        private int minZip;
        private int maxZip;


        public MinMaxAgeZip() {
            minAge = Integer.MAX_VALUE;
            maxAge = Integer.MIN_VALUE;
            minZip = Integer.MAX_VALUE;
            maxZip = Integer.MIN_VALUE;
        }

        public MinMaxAgeZip(MinMaxAgeZip minMax) {
            this.minAge = minMax.minAge;
            this.maxAge = minMax.maxAge;
            this.minZip = minMax.minZip;
            this.maxZip = minMax.maxZip;
        }


        @Override
        public String toString() {
            return "MinMaxAgeZip{" +
                    "minAge=" + minAge +
                    ", maxAge=" + maxAge +
                    ", minZip=" + minZip +
                    ", maxZip=" + maxZip +
                    '}';
        }


    }


    // TODO OLD
    public static List<Integer> findMinMaxAgeZip(List<Tuple> tuples) {

        List<Integer> res = new ArrayList<>();
        int minAge = Integer.MAX_VALUE;
        int maxAge = Integer.MIN_VALUE;
        int minZip = Integer.MAX_VALUE;
        int maxZip = Integer.MIN_VALUE;

        for (Tuple t : tuples) {
            if (t.getAge() < minAge) {
                minAge = t.getAge();
            }
            if (t.getAge() > maxAge) {
                maxAge = t.getAge();
            }

            if (Integer.parseInt(t.getZipcode()) < minZip) {
                minZip = Integer.parseInt(t.getZipcode());
            }
            if (Integer.parseInt(t.getZipcode()) > maxZip) {
                maxZip = Integer.parseInt(t.getZipcode());
            }
        }

        res.add(minAge);
        res.add(maxAge);
        res.add(minZip);
        res.add(maxZip);
        return res;
    }


    // TODO OLD
    private static double computeSchemesPerimeter(List<Tuple> list1, List<Tuple> list2, int ageRange, int zipRange) {

        List<Integer> listMinMaxAgeZip1 = findMinMaxAgeZip(list1);
        //int ageMax1 = list1.stream().map(Tuple::getAge).max(Integer::compareTo).get();
        //int ageMin1 = list1.stream().map(Tuple::getAge).min(Integer::compareTo).get();
        double agePerimeter1 = (listMinMaxAgeZip1.get(1) - listMinMaxAgeZip1.get(0)) * 1.0 / ageRange;

        List<Integer> listMinMaxAgeZip2 = findMinMaxAgeZip(list2);
        //int ageMax2 = list2.stream().map(Tuple::getAge).max(Integer::compareTo).get();
        //int ageMin2 = list2.stream().map(Tuple::getAge).min(Integer::compareTo).get();
        double agePerimeter2 = (listMinMaxAgeZip2.get(3) - listMinMaxAgeZip2.get(2)) * 1.0 / zipRange;

        return agePerimeter1 + agePerimeter2;
    }


    private static double computeSchemesPerimeter(MinMaxAgeZip minMax, int bucketSize, int ageRange, int zipRange) {
        double agePerimeter = (minMax.maxAge - minMax.minAge) * 1.0 / ageRange;
        double zipPerimeter = (minMax.maxZip - minMax.minZip) * 1.0 / zipRange;

        return (agePerimeter + zipPerimeter) * bucketSize * 1.0;
    }



    // TODO OLD
    public static AbstractMap.SimpleEntry<Double, List<List<Tuple>>> findBestSplitSchemeAge(Bucket bucket, int ageRange, int zipRange) {

        List<List<Tuple>> splitSchemes = new ArrayList<>();
        double bestPerimeter = Double.MAX_VALUE;

        List<Tuple> bestScheme1 = new ArrayList<>();
        List<Tuple> bestScheme2 = new ArrayList<>();

        int attributesCardinality = new ArrayList<>(bucket.getAttributes().entrySet()).get(0).getValue().size();

        //bucket.getAttributes().forEach((attr, tuples) -> {
        for (int i = 1; i < attributesCardinality; i++) {
            List<Tuple> list1 = new ArrayList<>();
            List<Tuple> list2 = new ArrayList<>();

            for (Map.Entry<String, List<Tuple>> attributes : bucket.getAttributes().entrySet()) {
                List<Tuple> tuples = attributes.getValue();
                tuples.sort(Comparator.comparing(Tuple::getAge));
                list1.addAll(tuples.subList(0, i));
                list2.addAll(tuples.subList(i, attributesCardinality));

                //splitSchemes.add(list1);
                //splitSchemes.add(list2);
            }

            double totalPerimeter = computeSchemesPerimeter(list1, list2, ageRange, zipRange);
            if (totalPerimeter < bestPerimeter) {
                bestPerimeter = totalPerimeter;
                bestScheme1 = list1;
                bestScheme2 = list2;
            }
        }

        splitSchemes.add(bestScheme1);
        splitSchemes.add(bestScheme2);
        return new AbstractMap.SimpleEntry<>(bestPerimeter, splitSchemes);
    }


    // TODO OLD
    public static AbstractMap.SimpleEntry<Double, List<List<Tuple>>> findBestSplitSchemeZip(Bucket bucket, int ageRange, int zipRange) {

        List<List<Tuple>> splitSchemes = new ArrayList<>();
        double bestPerimeter = Double.MAX_VALUE;

        List<Tuple> bestScheme1 = new ArrayList<>();
        List<Tuple> bestScheme2 = new ArrayList<>();

        int attributesCardinality = new ArrayList<>(bucket.getAttributes().entrySet()).get(0).getValue().size();

        //bucket.getAttributes().forEach((attr, tuples) -> {
        for (int i = 1; i < attributesCardinality; i++) {
            List<Tuple> list1 = new ArrayList<>();
            List<Tuple> list2 = new ArrayList<>();

            for (Map.Entry<String, List<Tuple>> attributes : bucket.getAttributes().entrySet()) {
                List<Tuple> tuples = attributes.getValue();
                tuples.sort(Comparator.comparing(Tuple::getZipcode));
                list1.addAll(tuples.subList(0, i));
                list2.addAll(tuples.subList(i, attributesCardinality));

                //splitSchemes.add(list1);
                //splitSchemes.add(list2);
            }

            double totalPerimeter = computeSchemesPerimeter(list1, list2, ageRange, zipRange);
            if (totalPerimeter < bestPerimeter) {
                bestPerimeter = totalPerimeter;
                bestScheme1 = list1;
                bestScheme2 = list2;
            }

            /*
            int zipMax1 = list1.stream().map(Tuple::getZipcode).map(Integer::parseInt).max(Integer::compareTo).get();
            int zipMin1 = list1.stream().map(Tuple::getZipcode).map(Integer::parseInt).min(Integer::compareTo).get();
            double perimeter1 = (zipMax1 - zipMin1) * 1.0 / zipRange;
            int zipMax2 = list2.stream().map(Tuple::getZipcode).map(Integer::parseInt).max(Integer::compareTo).get();
            int zipMin2 = list2.stream().map(Tuple::getZipcode).map(Integer::parseInt).min(Integer::compareTo).get();
            double perimeter2 = (zipMax2 - zipMin2) * 1.0 / zipRange;
*/
        }

        splitSchemes.add(bestScheme1);
        splitSchemes.add(bestScheme2);
        return new AbstractMap.SimpleEntry<>(bestPerimeter, splitSchemes);
    }


    private static void updateMinMaxAgeZip(MinMaxAgeZip minMax, Tuple t) {

        int tupleZip = Integer.parseInt(t.getZipcode());

        if (t.getAge() < minMax.minAge) {
            minMax.minAge = t.getAge();
        }
        if (t.getAge() > minMax.maxAge) {
            minMax.maxAge = t.getAge();
        }
        if (tupleZip < minMax.minZip) {
            minMax.minZip = tupleZip;
        }
        if(tupleZip > minMax.maxZip) {
            minMax.maxZip = tupleZip;
        }
    }


    public static List<MinMaxAgeZip> computeListMinMaxAgeZipReverse(List<List<Tuple>> attributesLists) {
        List<MinMaxAgeZip> res = new ArrayList<>();

        int attributesCardinality = attributesLists.get(0).size();
        MinMaxAgeZip minMax = new MinMaxAgeZip();

        for (int i = 1; i < attributesCardinality; i++) {
            List<Tuple> list1 = new ArrayList<>();
            List<Tuple> list2 = new ArrayList<>();

            for (List<Tuple> list : attributesLists) {
                Tuple t = list.get(attributesCardinality - i);
                updateMinMaxAgeZip(minMax, t);
            }

            MinMaxAgeZip copyMinMax = new MinMaxAgeZip(minMax);
            res.add(copyMinMax);
        }
        return res;
    }




    public static AbstractMap.SimpleEntry<Double, Integer> findBestSplitScheme(List<List<Tuple>> attributesList, int ageRange, int zipRange) {

        List<List<Tuple>> splitSchemes = new ArrayList<>();
        double bestPerimeter = Double.MAX_VALUE;
        int splitIndex = 1;
        int attributesCardinality = attributesList.get(0).size();
        MinMaxAgeZip minMax = new MinMaxAgeZip();

        // Compute minMax in reverse order
        List<MinMaxAgeZip> minMaxReverseList = computeListMinMaxAgeZipReverse(attributesList);
        int minMaxReverseSize = minMaxReverseList.size();

        // TODO commenter
        /*
        minMaxReverseList.forEach(System.out::println);
        System.out.println("WWWWWWWWWW\n");
        */


        //bucket.getAttributes().forEach((attr, tuples) -> {
        for (int i = 0; i < attributesCardinality - 1; i++) {

            for (int j = 0; j < attributesList.size(); j++) {
                Tuple t = attributesList.get(j).get(i);
                updateMinMaxAgeZip(minMax, t);
            }

            MinMaxAgeZip minMaxReverse = minMaxReverseList.get(minMaxReverseSize - (i + 1));

            double perimeter1 = computeSchemesPerimeter(minMax, (i + 1) * attributesList.size(), ageRange, zipRange);
            double perimeter2 = computeSchemesPerimeter(minMaxReverse, (attributesCardinality - (i + 1)) * attributesList.size(), ageRange, zipRange);
            double totalPerimeter = perimeter1 + perimeter2;

            if (totalPerimeter < bestPerimeter) {
                bestPerimeter = totalPerimeter;
                splitIndex = i + 1;
            }

        }

        return new AbstractMap.SimpleEntry<>(bestPerimeter, splitIndex);
    }




    public static List<Bucket> splitBucketOld(Bucket bucket, int ageRange, int zipRange){
        List<Bucket> completedBuckets = new ArrayList<>();
        List<Bucket> currentBuckets = new ArrayList<>();
        Set<String> bucketSignature = bucket.getSignature();
        int signatureSize = bucketSignature.size();
        currentBuckets.add(bucket);

        while (!currentBuckets.isEmpty()) {
            Bucket b = currentBuckets.get(0);
            if (b.size() == signatureSize) {
                completedBuckets.add(b);
                currentBuckets.remove(0);
                continue;
            }

            AbstractMap.SimpleEntry<Double, List<List<Tuple>>> bestSchemeAge = findBestSplitSchemeAge(b, ageRange, zipRange);
            AbstractMap.SimpleEntry<Double, List<List<Tuple>>> bestSchemeZip = findBestSplitSchemeZip(b, ageRange, zipRange);

            AbstractMap.SimpleEntry<Double, List<List<Tuple>>> bestScheme;
            if (bestSchemeAge.getKey() < bestSchemeZip.getKey()) {
                bestScheme = bestSchemeAge;
            }
            else {
                bestScheme = bestSchemeZip;
            }

            Bucket b1 = new Bucket(bucketSignature);
            List<Tuple> list1 = bestScheme.getValue().get(0);
            b1.addTuples(list1);
            Bucket b2 = new Bucket(b.getSignature());
            List<Tuple> list2 = bestScheme.getValue().get(1);
            b2.addTuples(list2);


            if (list1.size() == signatureSize) {
                completedBuckets.add(b1);
            }
            else {
                currentBuckets.add(b1);
            }

            if (list2.size() == signatureSize) {
                completedBuckets.add(b2);
            }
            else {
                currentBuckets.add(b2);
            }
            currentBuckets.remove(0);
        }


        return completedBuckets;
    }


    public static List<Bucket> createBuckets(Set<String> signature, List<List<Tuple>> attributesList, int splitIndex) {
        List<Bucket> res = new ArrayList<>();
        List<Tuple> l1 = new ArrayList<>();
        List<Tuple> l2 = new ArrayList<>();

        attributesList.forEach(l -> {
            for (int i = 0; i < splitIndex; i++) {
                l1.add(l.get(i));
            }
            for (int j = splitIndex; j < l.size(); j++) {
                l2.add(l.get(j));
            }
        });

        Bucket b1 = new Bucket(signature);
        b1.addTuples(l1);
        Bucket b2 = new Bucket(signature);
        b2.addTuples(l2);

        res.add(b1);
        res.add(b2);
        return res;
    }


    public static List<Bucket> splitBucket(Bucket bucket, int ageRange, int zipRange){
        List<Bucket> completedBuckets = new ArrayList<>();
        List<Bucket> currentBuckets = new ArrayList<>();
        Set<String> bucketSignature = bucket.getSignature();
        int signatureSize = bucketSignature.size();
        currentBuckets.add(bucket);

        while (!currentBuckets.isEmpty()) {
            Bucket b = currentBuckets.get(0);
            if (b.size() == signatureSize) {
                completedBuckets.add(b);
                currentBuckets.remove(0);
                continue;
            }

            List<List<Tuple>> attributesList = new ArrayList<>(b.getAttributes().values());

            // Sort attributes lists by age
            attributesList.forEach(l -> {
                l.sort(Comparator.comparing(Tuple::getAge));
            });
            //TODO Commenter
            //attributesList.forEach(System.out::println);
            //System.out.println("AAAAAAAAAAA");
            AbstractMap.SimpleEntry<Double, Integer> bestSchemeAge = findBestSplitScheme(attributesList, ageRange, zipRange);


            // Sort attributes lists by zip
            attributesList.forEach(l -> {
                l.sort(Comparator.comparing(Tuple::getZipcode));
            });
            //TODO Commenter
            //attributesList.forEach(System.out::println);
            //System.out.println("ZZZZZZZZZZZ");
            AbstractMap.SimpleEntry<Double, Integer> bestSchemeZip = findBestSplitScheme(attributesList, ageRange, zipRange);


            AbstractMap.SimpleEntry<Double, Integer> bestScheme;
            if (bestSchemeAge.getKey() < bestSchemeZip.getKey()) {
                bestScheme = bestSchemeAge;
                // We need to sort the list again because it is only a shallow copy
                attributesList.forEach(l -> {
                    l.sort(Comparator.comparing(Tuple::getAge));
                });
            }
            else {
                bestScheme = bestSchemeZip;
            }

            // TODO créer une fonction pour créer les 2 splits a partir du splitIndex

            List<Bucket> subBuckets = createBuckets(b.getSignature(), attributesList, bestScheme.getValue());
            Bucket b1 = subBuckets.get(0);
            Bucket b2 = subBuckets.get(1);

            if (b1.size() == signatureSize) {
                completedBuckets.add(b1);
            }
            else {
                currentBuckets.add(b1);
            }

            if (b2.size() == signatureSize) {
                completedBuckets.add(b2);
            }
            else {
                currentBuckets.add(b2);
            }
            currentBuckets.remove(0);
        }

        return completedBuckets;
    }





    // Applique split à tous les buckets
    public static List<Bucket> split(Map<Set<String>, Bucket> buckets, int ageRange, int zipRange) {
        List<Bucket> bucketsEquivalenceClasses = new ArrayList<>();

        buckets.forEach((signature, b) -> {
            List<Bucket> completedBuckets = splitBucket(b, ageRange, zipRange);
            bucketsEquivalenceClasses.addAll(completedBuckets);
        });
        return bucketsEquivalenceClasses;
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

        List<String> columnNames = new ArrayList<>();
        columnNames.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID>");
        columnNames.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#age>");
        columnNames.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode>");

        List<Boolean> isNumerical = new ArrayList<>();
        isNumerical.add(true);
        isNumerical.add(false);

        List<String> qidPredicates = new ArrayList<>();
        qidPredicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#age>");
        qidPredicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode>");

        List<String> hierarchies = new ArrayList<>();
        hierarchies.add("src/main/resources/hierarchies/age.csv");
        hierarchies.add("src/main/resources/hierarchies/zipcode.csv");

        String sensitivePredicate = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#disease>";

        List<Tuple> newTuples = new ArrayList<>();

        Tuple t1 = new Tuple("Emily", -1, 25, "21000", "Flu", false, false);
        Tuple t2 = new Tuple("Mary", -1, 46, "30000", "Gastro", false, false);
        Tuple t3 = new Tuple("Ray", -1, 54, "31000", "Dyspepsia", false, false);
        Tuple t4 = new Tuple("Tom", -1, 60, "44000", "Gastro", false, false);
        Tuple t5 = new Tuple("Vince", -1, 65, "36000", "Flu", false, false);
        newTuples.add(t1);
        newTuples.add(t2);
        newTuples.add(t3);
        newTuples.add(t4);
        newTuples.add(t5);


        int m = 2;


        List<Tuple> tuples = Division.retrieveAllTuples(predicates, model);
        System.out.println("TUPLES SIZE : " + tuples.size());
        List<Tuple> oldTuples = Division.filterOldTuples(tuples);
        System.out.println("OLD TUPLES SIZE : " + oldTuples.size());
        List<Tuple> aliveTuples = Division.filterAliveTuples(oldTuples);
        System.out.println("ALIVE TUPLES SIZE : " + aliveTuples.size());



        Map<Integer, List<Tuple>> eqClassesAlive = Division.createMapEquivalenceClasses(aliveTuples);
        Map<Integer, List<Tuple>> eqClassesAll = Division.createMapEquivalenceClasses(tuples);


        System.out.println("MAP SIZE : " + eqClassesAll.size());

        eqClassesAll.forEach((k,v) -> {
            System.out.println("CLASS " + k + " ==>  ");
            System.out.println(v.stream().map(Tuple::getId).collect(Collectors.joining(", ")));
        });
        System.out.println("\n\n");

        Map<Set<String>, List<Integer>> mapSignatures = Division.createMapSignatures(eqClassesAll, eqClassesAlive.keySet());
        mapSignatures.forEach((k,v) -> {
            System.out.println("Signature " + k + " ==>  " + v);
        });
        System.out.println("\n");

        Map<Set<String>, Bucket> buckets = Division.createBuckets(eqClassesAlive, mapSignatures);

/*
        buckets.forEach((k, v) -> {
            List<String> unba = v.getUnbalancedAttributes();
            System.out.println("SIGNATURE  " + k + " ==>  " + unba);
        });
        */



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
        Assignment.assignment(m, mapNewTuples, mapSaSize, buckets);


        buckets.forEach((k,v) -> {
            System.out.println("Bucket 2 " + k + " ==>  ");
            System.out.println(v);
            System.out.println("\n");
        });

        System.out.println("XXXXXXXXXXX");

        List<Bucket> finalBuckets = Split.split(buckets, 80, 25000);

        finalBuckets.forEach( x -> {
            System.out.println("Bucket 3 " + x.getSignature() + " ==>  ");
            System.out.println(x);
            System.out.println("\n");
        });



        String csvPath = "src/main/resources/test.csv";
        Counter blankIdCounter = new Counter();
        Counter classIdCounter = new Counter();

/*
        finalBuckets.forEach(fb -> {
            try {
                Generalization.globalGeneralization(model, , fb.getTuples(), qidPredicates, sensitivePredicate, columnNames, hierarchies,
                        isNumerical, csvPath, blankIdCounter, classIdCounter);
                classIdCounter.increment();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
*/

        String query = "DELETE {  ?s ?p ?o } WHERE { ?s ?p ?o . ?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 1}";
        UpdateAction.parseExecute(query, model);

        query = "INSERT DATA { []   <http://cool>  42 }";
        UpdateAction.parseExecute(query, model);

        query = "INSERT DATA { []   <http://cool>  42 }";
        UpdateAction.parseExecute(query, model);

        query = "SELECT * WHERE { ?s <http://cool>  ?o }";
        List<QuerySolution> qs = QueryUtil.execQuery(QueryFactory.create(query), model);
        qs.forEach(x -> {
            System.out.println("AAA -->  (" + x.get("s").toString() + ", " + x.get("o").toString());
        });



        try(OutputStream out = new FileOutputStream("src/main/resources/after.ttl", false)) {
            RDFDataMgr.write(out, model, Lang.TTL);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
