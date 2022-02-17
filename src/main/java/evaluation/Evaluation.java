package evaluation;

import algorithm.Tuple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import query.QueryUtil;

import java.util.*;
import java.util.stream.Collectors;

public class Evaluation {


    public static int retrieveAgeZipBeforeAnon(Model model, int minAge, int maxAge, String zipPrefix) {

        String sb = "SELECT * WHERE {\n" +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?o . " +
                "FILTER(?o >= " + minAge + " && ?o < " + maxAge + ") . " +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ?z . " +
                "FILTER regex(?z, \"^" + zipPrefix + "\") . " +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 0 . " +
                "}";
        Query q = QueryFactory.create(sb);
        return QueryUtil.execQuery(q, model).size();
    }


    public static int retrieveAgeZipAfterAnon(Model model, int minAge, int maxAge, String zipPrefix) {

        String sb1 = "SELECT * WHERE {\n" +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?o . " +
                "FILTER(?o >= " + minAge + " && ?o < " + maxAge + ") . " +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ?z . " +
                "FILTER regex(?z, \"^" + zipPrefix + "\") . " +
                "}";
        Query q = QueryFactory.create(sb1);
        int count1 = QueryUtil.execQuery(q, model).size();


        String sb2 = "SELECT * WHERE {\n" +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?b . " +
                "?b <http://minValue> ?min . " +
                "?b <http://maxValue> ?max . " +
                "FILTER(?min >= " + minAge + " && ?max < " + maxAge + ") . " +
                "?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ?z . " +
                "FILTER regex(?z, \"^" + zipPrefix + "\") .}";
        q = QueryFactory.create(sb2);
        int count2 = QueryUtil.execQuery(q, model).size();

        return count1 + count2;
    }



    public static double evaluate2(List<String> zipcodes, Model beforeModel, Model afterAnon) {
        List<Integer> ages = Arrays.asList(20, 30, 40, 50, 60, 70, 80, 90);
        double relativeErrors1 = 0.0;
        int count = 0;

        for (int i = 0; i < ages.size(); i++) {
            for (String z : zipcodes) {
                System.out.println("ZIP = " + z );
                int minValue = ages.get(i);

                int maxValue = minValue + 10;
                if (minValue == 90) {
                    maxValue++;
                }

                int before = retrieveAgeZipBeforeAnon(beforeModel, minValue, maxValue, z);
                int after = retrieveAgeZipAfterAnon(afterAnon, minValue, maxValue, z);
                System.out.println("BEFORE : " + before);
                System.out.println("AFTER : " + after);


                double relativeErr = ((before - after)*1.0) / (before * 1.0);
                if (relativeErr < 0) {
                    relativeErr *= -1;
                }
                relativeErrors1 += relativeErr;
                count++;
            }
        }

        return relativeErrors1 / count;
    }


    public static double evaluateRelativeError(Map<List<String>, Integer> mapBefore, Map<List<String>, Integer> mapAfter) {
        int i = 0;
        double relativeErrors = 0.0;
        for (Map.Entry<List<String>, Integer> entry : mapBefore.entrySet()) {
            if (entry.getValue() != 0) {
                int expectedCount = entry.getValue();
                int estimateCount = mapAfter.getOrDefault(entry.getKey(), 0);

                if (expectedCount < 0 || estimateCount < 0) {
                    System.out.println("Expected Value : " + expectedCount);
                    System.out.println("Estimate Value : " + estimateCount);
                }

                double relativeErr = ((expectedCount - estimateCount) * 1.0) / (expectedCount * 1.0);
                if (relativeErr < 0) {
                    relativeErr *= -1;
                }
                relativeErrors += relativeErr;
                i++;
            }
        }
        return relativeErrors / i;
    }

    public static Map<List<String>, Integer> computeMapAgeZipFromList(List<Tuple> tuples, int zipSuffixSize) {
        List<Integer> ages = Arrays.asList(20, 30, 40, 50, 60, 70, 80, 90);
        Map<List<String>, Integer> map = new HashMap<>();

        tuples.forEach(t -> {
            int zipSize = t.getZipcode().length();
            String zipPrefix = t.getZipcode().substring(0, zipSize - zipSuffixSize);
            t.setZipcode(zipPrefix);
        });

        for (int i = 0; i < ages.size(); i++) {
            int minValue = ages.get(i);
            int maxValue = minValue + 10;
            if (minValue == 90) {
                maxValue++;
            }

            int finalMaxValue = maxValue;
            Map<String, List<Tuple>> groupByZip =  tuples.stream().filter(t -> t.getAge() >= minValue && t.getAge() < finalMaxValue).collect(Collectors.groupingBy(Tuple::getZipcode));

            groupByZip.forEach((zip, t) -> {
                List<String> attributes = new ArrayList<>();
                attributes.add(String.valueOf(minValue));
                attributes.add(String.valueOf(finalMaxValue));
                attributes.add(zip);
                map.put(attributes, t.size());
            });
         }

        return map;
    }



    public static void updateAddMapAgeZip(Map<List<String>, Integer> map1, Map<List<String>, Integer> map2) {
        map1.forEach((attr1, count1) -> {
            map2.computeIfPresent(attr1, (attr2, count2) -> count1 + count2);
            map2.computeIfAbsent(attr1, s -> count1);
        });
    }


    public static void updateDeleteMapAgeZip(Map<List<String>, Integer> map1, Map<List<String>, Integer> map2) {
        map1.forEach((attr1, count1) -> {
            map2.compute(attr1, (attr2, count2) -> {
                if (count1 > count2) {
                    System.out.println("@@@@@@@@@\n" + attr1);
                    System.out.println("C1 : " + count1);
                    System.out.println("C2 : " + count2);
                }
                return count2 - count1;
            });
        });
    }



    public static String createCountAgeZipQuery(int minAgeValue, int maxAgeValue) {

        StringBuilder queryStr = new StringBuilder("SELECT ?z (COUNT(?s) as ?c) ");

        queryStr.append("WHERE { { ");
        queryStr.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?o . ")
                .append("FILTER(?o >= ").append(minAgeValue).append(" && ?o < ").append(maxAgeValue).append(") . ");
        queryStr.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ?z . \n");

        queryStr.append(" } UNION { \n");

        queryStr.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?b . ")
                .append("?b <http://minValue> ?min . ")
                .append("?b <http://maxValue> ?max . ")
                .append("FILTER(?min >= ").append(minAgeValue).append(" && ?max < ").append(maxAgeValue).append(") . ");
        queryStr.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ?z . \n}} ");

        queryStr.append("GROUP BY ?z");

        return queryStr.toString();
    }


    private static void computeCountAgeZipAfterAnon(Model model, Integer minAgeValue, Integer maxAgeValue, Map<List<String>, Integer> mapAfterAnon, int zipSuffixSize) {

        Query q = QueryFactory.create(createCountAgeZipQuery(minAgeValue, maxAgeValue));
        QueryUtil.execQuery(q, model).forEach(qs -> {
            List<String> attributesValues = new ArrayList<>();
            attributesValues.add(minAgeValue.toString());
            attributesValues.add(maxAgeValue.toString());
            String zipcode = qs.get("z").toString();
            String zipcodePrefix = zipcode.substring(0, zipcode.length() - zipSuffixSize);
            attributesValues.add(zipcodePrefix);
            int count = Integer.parseInt(qs.get("c").toString().split("\\^\\^")[0]);
            mapAfterAnon.computeIfPresent(attributesValues, (attr, currentCount) -> currentCount + count);
            mapAfterAnon.putIfAbsent(attributesValues, count);
        });
    }



    public static Map<List<String>, Integer> createMapCountAgeZipAfterAnon(Model model, int zipSuffixSize) {
        List<Integer> ages = Arrays.asList(20, 30, 40, 50, 60, 70, 80, 90);
        Map<List<String>, Integer> mapAfterAnon = new HashMap<>();

        for (int i = 0; i < ages.size(); i++) {
            int minValue = ages.get(i);
            int maxValue = minValue + 10;
            if (minValue == 90) {
                maxValue++;
            }
            computeCountAgeZipAfterAnon(model, minValue, maxValue, mapAfterAnon, zipSuffixSize);
        }
        return mapAfterAnon;
    }






    public static void main(String[] args) {

        List<Tuple> tuples = new ArrayList<>();

        Tuple t1 = new Tuple("A", -1, 25, "aa", "Flu", false, false);
        Tuple t2 = new Tuple("B", -1, 36, "bb", "Gastro", false, false);
        Tuple t3 = new Tuple("C", -1, 44, "bb", "Dyspepsia", false, false);
        Tuple t4 = new Tuple("D", -1, 22, "aa", "Gastro", false, false);
        Tuple t5 = new Tuple("E", -1, 46, "bb", "Flu", false, false);
        Tuple t6 = new Tuple("F", -1, 34, "cc", "Dyspepsia", false, false);
        Tuple t7 = new Tuple("G", -1, 20, "bb", "Gastro", false, false);
        Tuple t8 = new Tuple("H", -1, 44, "bb", "Flu", false, false);
        Tuple t9 = new Tuple("I", -1, 33, "dd", "Gastro", false, false);
        Tuple t10 = new Tuple("J", -1, 26, "bb", "Flu", false, false);


        tuples.add(t1);
        tuples.add(t2);
        tuples.add(t3);
        tuples.add(t4);
        tuples.add(t5);
        tuples.add(t6);
        tuples.add(t7);
        tuples.add(t8);
        tuples.add(t9);
        tuples.add(t10);

        System.out.println("TUPLES SIZE ==>  " + tuples.size());


        Map<List<String>, Integer> m = computeMapAgeZipFromList(tuples, 0);
        m.forEach((k, v) -> System.out.println(k + " ==> " + v));


        Tuple t42 = new Tuple("J", -1, 66, "ee", "Flu", false, false);


        List<Tuple> tuples2 = new ArrayList<>();
        tuples2.add(t1);
        tuples2.add(t4);
        tuples2.add(t2);
        tuples2.add(t42);
        Map<List<String>, Integer> m2 = computeMapAgeZipFromList(tuples2, 0);
        System.out.println("\nXXXXXXXXXXXXXXXXXXX\n");
        updateDeleteMapAgeZip(m2, m);
        System.out.println("\nXXXXXXXXXXXXXXXXXXX\n");
        m.forEach((k, v) -> System.out.println(k + " ==> " + v));

    }






}
