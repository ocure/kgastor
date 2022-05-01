import algorithm.*;
import evaluation.Evaluation;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.update.UpdateAction;
import query.QueryUtil;
import reader.Reader;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
    }



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

        String sensitivePredicate = "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#hasReligion>";


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
            tuples.remove(tuples.size() - 1);
        }
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Remove tuples from First Time : " + elapsed);


        start = System.currentTimeMillis();
        Map<List<String>, Integer> mapAgeZip = Evaluation.computeMapAgeZipFromList(tuples, zipSuffixSize);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("Compute First AgeZip Map : " + elapsed);


        // Première publication
        double totalTime = 0.0;
        double relError = 0.0;
        start = System.currentTimeMillis();
        Generalization.apply_M_Invariance(privateGraph, defaultGraph, predicates, qidPredicates, sensitivePredicate, m, zipRange,
               columnNames, hierarchies, isNumerical, csvPath, maxNbCounterfeits, totalNbCounterfeits);
        elapsed = (System.currentTimeMillis() - start) / 1000.0;
        System.out.println("FIRST PUBLI TIME : " + elapsed);
        totalTime += elapsed;


        // EVAL
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

        System.out.println("RELATIVE ERROR  :  " + relError + "\n");

        int count = 1;
        int updateVolume = tuples.size() / rate;
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

            System.out.println("DEFAULT SIZE : " + defaultGraph.size());
            System.out.println("PRIVATE SIZE : " + privateGraph.size());

            System.out.println("RELATIVE ERROR  :  " + relError + "\n");
        }

        System.out.println("\nXXXXXXXXXXXXXXXX\nTOTAL EXEC TIME FOR ALL M-INVARIANCE  ==> " + totalTime);
        System.out.println("TOTAL RELATIVE ERROR  ==> " + totalRelativeError/count);

        System.out.println("TOTAL NUMBER OF COUNTERFEIT TUPLES  ==>  " + totalNbCounterfeits.get());
        System.out.println("AVERAGE NUMBER OF COUNTERFEIT TUPLES  ==>  " + totalNbCounterfeits.get() / count);
        System.out.println("MAX NUMBER OF COUNTERFEIT TUPLES  ==>  " + maxNbCounterfeits.get());


        if (dataset != null) {
            dataset.abort();
            dataset.end();
        }
    }
}
