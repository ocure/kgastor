import algorithm.*;
import evaluation.Evaluation;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.update.UpdateAction;
import query.QueryUtil;
import reader.Reader;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Main {


    public static List<String> retrieveProfessors(Model model) {
        List<String> fullProfs = QueryUtil.extractFullProfessorsFromGraph(model);
        List<String> assiProfs = QueryUtil.extractAssistantProfessorsFromGraph(model);
        List<String> assoProfs = QueryUtil.extractAssociateProfessorsFromGraph(model);

        List<String> profs = new ArrayList<>(fullProfs);
        profs.addAll(assiProfs);
        profs.addAll(assoProfs);
        System.out.println("TOTAL PROFS " +  profs.size());
        return profs;
    }


    public static void prepareProfessorNodes(List<String> professors, Model model) {

        professors.forEach(p -> {
            StringBuilder sb = new StringBuilder("INSERT DATA {\n");
            sb.append("<").append(p).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> -1 .\n");
            sb.append("<").append(p).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 0 .\n}");
            UpdateAction.parseExecute(sb.toString(), model);
        });
        //sb.append("}");
        //UpdateAction.parseExecute(sb.toString(), model);
    }


/*
    private static void removeTuplesFromModel(List<Tuple> tuples, Model model) {

        String filterExpression = tuples.stream().map(t -> {
            return "?s = <" + t.getId() + ">";
        }).collect(Collectors.joining(" || "));

        String query = "DELETE {?s ?p ?o} WHERE {?s ?p ?o . FILTER("+ filterExpression + ").}";
        UpdateAction.parseExecute(query, model);
    }


    private static void updateDeleteTombstones(List<Tuple> tuples, Model model) {
        String filterExpression = tuples.stream().map(t -> {
            return "?s = <" + t.getId() + ">";
        }).collect(Collectors.joining(" || "));

        String query = "DELETE {?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> ?o} " +
                "INSERT {?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 1} " +
                "WHERE {?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> ?o . FILTER("+ filterExpression + ").}";

        UpdateAction.parseExecute(query, model);
    }
*/

    private static void addTuplesToModel(List<Tuple> tuples, String sensitivePredicate, Model model) {
        StringBuilder sb = new StringBuilder("INSERT DATA {\n");
        for (Tuple t : tuples) {
            sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ").append(t.getAge()).append(" .\n");
            sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> \"").append(t.getZipcode()).append("\" .\n");
            sb.append("<").append(t.getId()).append("> ").append(sensitivePredicate).append(" \"").append(t.getSa()).append("\" .\n");
            sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> -1 .\n");
            sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 0").append(" .\n");
        }
        sb.append("}");

        //System.out.println(sb.toString() + "}\n");
        UpdateAction.parseExecute(sb.toString(), model);
    }





    // TODO New Version
    private static void removeTuplesFromModel(List<Tuple> tuples, Model model) {
        tuples.forEach(t -> {
            String query = "DELETE {?s ?p ?o} WHERE {?s ?p ?o . FILTER(?s = <"+ t.getId() + ">).}";
            UpdateAction.parseExecute(query, model);
        });
    }


    // TODO new version
    private static void updateDeleteTombstones(List<Tuple> tuples, Model model) {
        for (Tuple t : tuples) {
            String query = "DELETE { <" + t.getId() + "> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> ?o} " +
                    "INSERT { <" + t.getId() + "> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 1} " +
                    "WHERE { <" + t.getId() + "> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> ?o .}";
            UpdateAction.parseExecute(query, model);
        }
    }




    private static List<Tuple> removeTuplesFromList(List<Tuple> tuples, int r) {
        List<Tuple> res = new ArrayList<>(tuples.subList(tuples.size() - r, tuples.size()));
        for (int i = 0; i < r; i ++) {
            tuples.remove(tuples.size() - 1);
        }
        return res;
    }



    public static void main(String[] args) {


        List<String> columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#age>");
        columnNames.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode>");

        List<Boolean> isNumerical = new ArrayList<>();
        isNumerical.add(true);
        isNumerical.add(false);

        List<String> qidPredicates = new ArrayList<>();
        qidPredicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#age>");
        qidPredicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode>");

        List<String> hierarchies = new ArrayList<>();
        //hierarchies.add("src/main/resources/hierarchies/age.csv");
        //hierarchies.add("src/main/resources/hierarchies/zipcode.csv");

        String sensitivePredicate = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#hasReligion>";

        /*
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
*/

        Dataset dataset = null;
        Model privateGraph = ModelFactory.createDefaultModel();
        Model defaultGraph = ModelFactory.createDefaultModel();
        int m = 3;
        int zipRange = 3000;
        String csvPath = null;
        int rate = 4;
        int zipSuffixSize = 0;
        AtomicInteger totalNbCounterfeits = new AtomicInteger(0);
        AtomicInteger maxNbCounterfeits = new AtomicInteger(0);


        int tuplePoolSize = 100 ;
        int nbLoops = 2;


        String arg;
        try {
            for (int i = 0; i < args.length; i++) {
                arg = args[i++];
                System.out.println("Option : " + arg);
                switch (arg) {
                    case "-d":
                        while (i < args.length && !args[i].startsWith("-")){
                            Reader.readModelFromDirectory(args[i], privateGraph, null);
                            System.out.println(args[i]);
                            i++;
                        }
                        i--;
                        break;
                    case "-f":
                        while (i < args.length && !args[i].startsWith("-")){
                            privateGraph.read(args[i]);
                            System.out.println(args[i]);
                            i++;
                        }
                        i--;
                        break;
                    case "-tdb":
                        System.out.println(args[i]);
                        String file = args[i];
                        /*Dataset ds = TDB2Factory.createDataset();
                        Txn.executeWrite(ds, ()->{
                            RDFDataMgr.read(ds, file);
                        });
                        dataset = ds;*/
                        dataset = TDBFactory.createDataset(file);
                        privateGraph = dataset.getDefaultModel();
                        System.out.println("PRIVATE SIZE :" + privateGraph.size());
                        break;
                    case "-hierarchies":
                        while (i < args.length && !args[i].startsWith("-")){
                            hierarchies.add(args[i]);
                            System.out.println(args[i]);
                            i++;
                        }
                        i--;
                        break;
                    case "-csv_path":
                        System.out.println(args[i]);
                        csvPath = args[i];
                        break;
                    case "-sensitive":
                        System.out.println(args[i]);
                        sensitivePredicate = args[i];
                        break;
                    case "-m":
                        m = Integer.parseInt(args[i]);
                        System.out.println(args[i]);
                        break;
                    case "-zip_range":
                        zipRange = Integer.parseInt(args[i]);
                        System.out.println(args[i]);
                        break;
                    case "-ratio":
                        rate = Integer.parseInt(args[i]);
                        System.out.println(args[i]);
                        break;
                    case "-pool":
                        tuplePoolSize = Integer.parseInt(args[i]);
                        System.out.println(args[i]);
                        break;
                    case "-i":
                        nbLoops = Integer.parseInt(args[i]);
                        System.out.println(args[i]);
                        break;
                    case "-zipSuffix":
                        zipSuffixSize = Integer.parseInt(args[i]);
                        break;
                    default:
                        System.out.println("Wrong option : " + arg);
                        break;
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }

        List<String> predicates = new ArrayList<>();
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID>");
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#age>");
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode>");
        predicates.add(sensitivePredicate);
        predicates.add("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone>");




        if (dataset != null) {
            dataset.begin(ReadWrite.WRITE);
        }

        System.out.println("PRIVATE SIZE AFTER QID/SA : " + privateGraph.size());



        double start = System.currentTimeMillis();
        List<String> professors = retrieveProfessors(privateGraph);
        prepareProfessorNodes(professors, privateGraph);
        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("\nPREPARATION TIME : " + elapsed + "\n");


        //TODO enlever partie des profs dans pool

        // We shuffle the professors and create the pool from the last tuples in the list
        List<Tuple> tuples = Division.retrieveAllTuples(predicates, privateGraph);
        start = System.currentTimeMillis();
        Collections.shuffle(tuples);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Shuffling Time : " + elapsed);

        int middle = tuples.size() / 2;
        start = System.currentTimeMillis();
        List<Tuple> insertPool = new ArrayList<>(tuples.subList(middle, tuples.size()));
        removeTuplesFromModel(insertPool, privateGraph);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Remove tuples from model Time : " + elapsed);

        start = System.currentTimeMillis();
        for (int i = 0; i < insertPool.size(); i++) {
            //System.out.println(insertPool.get(i).getId());
            tuples.remove(tuples.size() - 1);
        }
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Remove tuples from First Time : " + elapsed);


        start = System.currentTimeMillis();
        Map<List<String>, Integer> mapAgeZip = Evaluation.computeMapAgeZipFromList(tuples, zipSuffixSize);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Compute First AgeZip Map : " + elapsed);


        // TODO première publication

        double totalTime = 0.0;
        double relError = 0.0;
        start = System.currentTimeMillis();
        Generalization.apply_M_Invariance(privateGraph, defaultGraph, predicates, qidPredicates, sensitivePredicate, m, zipRange,
               columnNames, hierarchies, isNumerical, csvPath, maxNbCounterfeits, totalNbCounterfeits);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("FIRST PUBLI TIME : " + elapsed);
        totalTime += elapsed;


        //TODO EVAL
        start = System.currentTimeMillis();
        Map<List<String>, Integer> mapAfter = Evaluation.createMapCountAgeZipAfterAnon(defaultGraph, zipSuffixSize);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Compute AFTER AgeZip Map : " + elapsed);
        double totalRelativeError = 0.0;

        start = System.currentTimeMillis();
        relError = Evaluation.evaluateRelativeError(mapAgeZip, mapAfter);
        totalRelativeError += relError;
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("EVALUATING RELATIVE ERROR : " + elapsed);

        long ageZipBeforeSize = mapAgeZip.entrySet().stream().filter(e -> e.getValue() != 0).count();
        int ageZipAfterSize = mapAfter.size();
/*
        mapAgeZip.forEach((k,v) -> System.out.println(k + " ==>  " + v));
        System.out.println("\nXXXXXXXXXXXXXXXXXX\n");
        mapAfter.forEach((k,v) -> System.out.println(k + " ==>  " + v));
*/
        System.out.println("RELATIVE ERROR  :  " + relError + "\n");

        int count = 1;

        /*
        List<String> zipcodes = QueryUtil.getZipcodes(privateGraph).stream().map(z -> {
            int prefixSize = z.length() - 3;
            return z.substring(0, prefixSize);
        }).distinct().collect(Collectors.toList());
        relError += Evaluation.evaluate(zipcodes, privateGraph, defaultGraph);
        int count = 1;
        */

        // TODO boucle avec nbloops

        int updateVolume = tuples.size() / rate;
        // TODO CHANGE BOUCLE avec rate
        for (int i = 0; i < rate; i++) {
            System.out.println("----------\nLOOP  " + i + "\n");

            start = System.currentTimeMillis();
            Collections.shuffle(tuples);
            Collections.shuffle(insertPool);
            List<Tuple> newTuples = removeTuplesFromList(insertPool, updateVolume);
            List<Tuple> tuplesToDelete = removeTuplesFromList(tuples, updateVolume);
            elapsed = (System.currentTimeMillis() - start) / 1000.0;
            System.out.println("Preparing tuples to delete and new tuples Time : " + elapsed);


            start = System.currentTimeMillis();
            updateDeleteTombstones(tuplesToDelete, privateGraph);
            addTuplesToModel(newTuples, sensitivePredicate, privateGraph);
            elapsed = (System.currentTimeMillis() - start) / 1000.0;
            System.out.println("UPDATE TOMBSTONE TIME : " + elapsed);


            start = System.currentTimeMillis();
            //TODO delete les blanks nodes pour l'âge avant
            Generalization.apply_M_Invariance(privateGraph, defaultGraph, predicates, qidPredicates, sensitivePredicate, m, zipRange,
                    columnNames, hierarchies, isNumerical, csvPath, maxNbCounterfeits, totalNbCounterfeits);

            elapsed = (System.currentTimeMillis() - start) / 1000.0;
            System.out.println("M-INVARIANCE TIME : " + elapsed);
            totalTime += elapsed;

            start = System.currentTimeMillis();
            Map<List<String>, Integer> mapAgeZipNew = Evaluation.computeMapAgeZipFromList(newTuples, zipSuffixSize);
            Map<List<String>, Integer> mapAgeZipDelete = Evaluation.computeMapAgeZipFromList(tuplesToDelete, 0);
            Evaluation.updateAddMapAgeZip(mapAgeZipNew, mapAgeZip);
            Evaluation.updateDeleteMapAgeZip(mapAgeZipDelete, mapAgeZip);
            elapsed = (System.currentTimeMillis() - start) / 1000.0;
            System.out.println("Compute AgeZip from list : " + elapsed);


            // TODO query the graph to retrieve approximate count values and create another map

            start = System.currentTimeMillis();
            mapAfter = Evaluation.createMapCountAgeZipAfterAnon(defaultGraph, zipSuffixSize);
            elapsed = (System.currentTimeMillis() - start) / 1000.0;
            System.out.println("Compute AFTER AgeZip Map : " + elapsed);


            ageZipBeforeSize = mapAgeZip.entrySet().stream().filter(e -> e.getValue() != 0).count();
            ageZipAfterSize = mapAfter.size();
            System.out.println("Map Before SIZE: " + ageZipBeforeSize);
            System.out.println("Map After SIZE: " + ageZipAfterSize);



            start = System.currentTimeMillis();
            relError = Evaluation.evaluateRelativeError(mapAgeZip, mapAfter);
            totalRelativeError += relError;
            count++;
            elapsed = (System.currentTimeMillis() - start) / 1000.0;
            System.out.println("EVALUATING RELATIVE ERROR : " + elapsed);

            /*
            mapAgeZip.forEach((k,v) -> System.out.println(k + " ==>  " + v));
            System.out.println("\nXXXXXXXXXXXXXXXXXX\n");
            mapAfter.forEach((k,v) -> System.out.println(k + " ==>  " + v));
            */

            System.out.println("DEFAULT SIZE : " + defaultGraph.size());
            System.out.println("PRIVATE SIZE : " + privateGraph.size());


            System.out.println("RELATIVE ERROR  :  " + relError + "\n");

            // TODO iterate through main map and compute relative errors

            /*
            zipcodes = QueryUtil.getZipcodes(privateGraph).stream().map(z -> {
                int prefixSize = z.length() - 3;
                return z.substring(0, prefixSize);
            }).distinct().collect(Collectors.toList());
            count++;
            relError += Evaluation.evaluate(zipcodes, privateGraph, defaultGraph);
*/
        }

        System.out.println("\nXXXXXXXXXXXXXXXX\nTOTAL EXEC TIME FOR ALL M-INVARIANCE  ==> " + totalTime);
        System.out.println("TOTAL RELATIVE ERROR  ==> " + totalRelativeError/count);

        // TODO print max nb fakes et nb total fakes
        System.out.println("TOTAL NUMBER OF COUNTERFEIT TUPLES  ==>  " + totalNbCounterfeits.get());
        System.out.println("AVERAGE NUMBER OF COUNTERFEIT TUPLES  ==>  " + totalNbCounterfeits.get() / count);
        System.out.println("MAX NUMBER OF COUNTERFEIT TUPLES  ==>  " + maxNbCounterfeits.get());



/*
        try(OutputStream out = new FileOutputStream("default.ttl", false)) {
            RDFDataMgr.write(out, defaultGraph, Lang.TTL);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
*/
        if (dataset != null) {
            dataset.abort();
            dataset.end();
        }


        // TODO Commenter
        /*
        try(OutputStream out = new FileOutputStream("default.ttl", false)) {
            RDFDataMgr.write(out, defaultGraph, Lang.TTL);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
*/


/*
        List<Tuple> tuples = Division.retrieveAllTuples(predicates, privateGraph);
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

        Map<Set<String>, List<Integer>> mapSignatures = Division.createMapSignatures(eqClassesAll);
        mapSignatures.forEach((k,v) -> {
            System.out.println("Signature " + k + " ==>  " + v);
        });
        System.out.println("\n");

        Map<Set<String>, Bucket> buckets = Division.createBuckets(eqClassesAlive, mapSignatures);

        //TODO method pour récupérer new tuples
        List<Tuple> newTuples = Division.filterNewTuples(tuples);
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

        // TODO param pour zipRange
        List<Bucket> finalBuckets = Split.split(buckets, 80, 25000);

        finalBuckets.forEach( x -> {
            System.out.println("Bucket 3 " + x.getSignature() + " ==>  ");
            System.out.println(x);
            System.out.println("\n");
        });


        String query = "DELETE {  ?s ?p ?o } WHERE { ?s ?p ?o . ?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 1}";
        UpdateAction.parseExecute(query, privateGraph);


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
*/
    }


}
