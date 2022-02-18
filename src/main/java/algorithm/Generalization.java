package algorithm;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.update.UpdateAction;
import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.exceptions.RollbackRequiredException;
import org.deidentifier.arx.metric.Metric;
import query.QueryUtil;
import util.ArxUtil;
import util.Counter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Generalization {



    public static void flushTuplesToCsv(List<Tuple> tuples, List<String> columnNames, String filepath) throws IOException {
        Path p = Paths.get(filepath);
        String columnNamesLine = columnNames.stream().collect(Collectors.joining(";", "", "\n"));
        Files.write(p, columnNamesLine.getBytes());
        StringBuilder sb = new StringBuilder();
        for (Tuple t : tuples) {
            sb.append(t.getId()).append(";").append(t.getAge()).append(";").append(t.getZipcode()).append("\n");
        }
        Files.write(p, sb.toString().getBytes(), StandardOpenOption.APPEND);
    }


    public static void globalGeneralization(Model defaultModel, Model privateModel, List<Tuple> tuples, List<String> predicates, String sensitivePredicate, List<String> columnNames,
                                            List<String> hierarchies, List<Boolean> isNumerical,
                                            String csvFilepath,Counter blankIdCounter, Counter classIdCounter) throws IOException {

        flushTuplesToCsv(tuples, columnNames, csvFilepath);

        Data data = Data.create(csvFilepath, StandardCharsets.UTF_8, ';');
        ArxUtil.setDataAttributeTypes(data, columnNames);
        ArxUtil.setDataHierarchies(data, columnNames, hierarchies);


        // Define relative number of records to be generalized in each iteration
        double oMin = 0.01d;

        // Define a parameter for the quality model which only considers generalization
        double gsFactor = 0d;

        ARXAnonymizer anonymizer = new ARXAnonymizer();
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new KAnonymity(tuples.size()));
        config.setSuppressionLimit(0d);
        //config.setSuppressionLimit(1d - oMin);
        config.setQualityModel(Metric.createLossMetric(gsFactor));

        /*
        config.setAttributeWeight("age", 0.5);
        config.setAttributeWeight("sex", 0.2);
        config.setAttributeWeight("zipcode", 0.3);
        */

        ARXResult result = anonymizer.anonymize(data, config);

        DataHandle optimum = result.getOutput();

        try {
            // Now apply local recoding to the result
            result.optimizeIterativeFast(optimum, 0.1d);
        } catch (RollbackRequiredException e) {

            // This part is important to ensure that privacy is preserved, even in case of exceptions
            optimum = result.getOutput();
        }

        Iterator<String[]> transformed = optimum.iterator();
        List<List<String>> transformedData = new ArrayList<>();
        transformed.next();
        transformed.forEachRemaining(e -> transformedData.add(Arrays.asList(e)));

        // TODO supprimer dans default les triplets age et zipcode
        // TODO dans computeGeneralization, on fait uniquement des inserts dans default et des delete/insert dans private
        List<List<String>> updateQueries = QueryUtil.computeGeneralizationQueries(tuples, transformedData, predicates, sensitivePredicate, isNumerical, blankIdCounter, classIdCounter);
        List<String> updatePrivate = updateQueries.get(0);
        List<String> updateDefault = updateQueries.get(1);

        updateDefault.forEach(x -> {
            UpdateAction.parseExecute(x, defaultModel);
        });
        updatePrivate.forEach(x -> UpdateAction.parseExecute(x, privateModel));
    }




    //TODO execute m-invariance
    public static void apply_M_Invariance(Model privateGraph, Model defaultGraph, List<String> predicates,
                                          List<String> qidPredicates, String sensitivePredicate, int m, int zipRange,
                                          List<String> columnNames, List<String> hierarchies, List<Boolean> isNumerical,
                                          String csvPath, AtomicInteger maxNbCounterfeits, AtomicInteger totalNbCounterfeits) {

        double start = System.currentTimeMillis();

        List<Tuple> tuples = Division.retrieveAllTuples(predicates, privateGraph);
        List<Tuple> oldTuples = Division.filterOldTuples(tuples);
        List<Tuple> newTuples = Division.filterNewTuples(tuples);
        List<Tuple> aliveTuples = Division.filterAliveTuples(oldTuples);
        List<Tuple> deadTuples = Division.filterDeadTuples(tuples);

        System.out.println("TUPLES SIZE : " + tuples.size());
        System.out.println("OLD TUPLES SIZE : " + oldTuples.size());
        System.out.println("NEW TUPLES SIZE : " + newTuples.size());
        System.out.println("ALIVE TUPLES SIZE : " + aliveTuples.size());
        System.out.println("DEAD TUPLES SIZE : " + deadTuples.size());


        Map<Integer, List<Tuple>> eqClassesAlive = Division.createMapEquivalenceClasses(aliveTuples);
        Map<Integer, List<Tuple>> eqClassesAll = Division.createMapEquivalenceClasses(oldTuples);
        Map<Set<String>, List<Integer>> mapSignatures = Division.createMapSignatures(eqClassesAll, eqClassesAlive.keySet());

        Map<Set<String>, Bucket> buckets = Division.createBuckets(eqClassesAlive, mapSignatures);
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("DIVISION TIME : " + elapsed);


        start = System.currentTimeMillis();
        System.out.println();
        Map<String, List<Tuple>> mapNewTuples = Balancing.groupTuplesBySa(newTuples);

        int indexFakeCounter = 0;
        for (Map.Entry<Set<String>, Bucket> setBucketEntry : buckets.entrySet()) {
            Bucket b = setBucketEntry.getValue();
            indexFakeCounter = b.balance(mapNewTuples, m, indexFakeCounter);
            int currentMaxNbCounterfeit = maxNbCounterfeits.get();
            if (indexFakeCounter > currentMaxNbCounterfeit) {
                maxNbCounterfeits.set(indexFakeCounter);
            }
        }
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("BALANCING TIME : " + elapsed);
        System.out.println("NUMBER OF  BUCKETS  BEFORE ASSIGN==>  " + buckets.size());

        System.out.println("======\nNB FAKES ==>  " + indexFakeCounter + "\n========");
        totalNbCounterfeits.addAndGet(indexFakeCounter);

        start = System.currentTimeMillis();
        Map<String, Integer> mapSaSize = Balancing.computeMapSaSize(mapNewTuples);
        Assignment.assignment(m, mapNewTuples, mapSaSize, buckets);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("ASSIGNMENT TIME : " + elapsed);

        System.out.println("NUMBER OF  BUCKETS  AFTER ASSIGN==>  " + buckets.size());

        start = System.currentTimeMillis();
        List<Bucket> finalBuckets = Split.split(buckets, 80, zipRange);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("SPLIT TIME : " + elapsed);


        // Delete tuples with tombstone 1
        String query = "DELETE {  ?s ?p ?o } WHERE { ?s ?p ?o . ?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 1}";
        System.out.println("PRIVATE GRAPH SIZE BEFORE : " + privateGraph.size());
        UpdateAction.parseExecute(query, privateGraph);
        //QueryUtil.deleteTuplesFromModel(deadTuples, sensitivePredicate, privateGraph);
        System.out.println("PRIVATE GRAPH SIZE AFTER : " + privateGraph.size());


        // Delete qids in default
        // TODO DO NOT DELETE SA
        String deleteClause = QueryUtil.createDeleteClause(qidPredicates, sensitivePredicate, isNumerical);
        String whereClause =  QueryUtil.createUpdateWhereClause(qidPredicates, sensitivePredicate, isNumerical);
        String deleteQidQueryString = "DELETE {\n" + deleteClause + "}\nWHERE {\n" + whereClause + "}";
        UpdateAction.parseExecute(deleteQidQueryString, defaultGraph);

        Counter blankIdCounter = new Counter();
        Counter classIdCounter = new Counter();

        for (Bucket fb : finalBuckets) {
            try {
                Generalization.globalGeneralization(defaultGraph, privateGraph, fb.getTuples(), qidPredicates, sensitivePredicate, columnNames, hierarchies,
                        isNumerical, csvPath, blankIdCounter, classIdCounter);
                classIdCounter.increment();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
