package kgastor.main;
import kgastor.utils.Evaluation;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Main {
    static Evaluation evaluation = new Evaluation();
    static long startTime, stopTime;
    public static void main(String args[]) {
        if (args.length > 0) {
            switch (args[0]) {
                case "select":
                    System.out.println("select Query: dbName, queryFile");
                    SelectQr selectQr = new SelectQr(args[1],args[2]);
                    selectQr.query();
                    break;
                case "noPart":
                    System.out.println("Loading without partitioning");
                    try {
                        noPartition(args[1]);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                case "part":
                    System.out.println("Loading with partitioning");
                    try {
                        partition(args[1]);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }

                    break;
                default:
                    System.out.println("Error, one of select, noPart, part is required");
            }
        }
        else
            System.out.println("Parameters required: select db queryFile\n noPart ");
    }

    public static void noPartition(String dbName)  throws IOException {
        startTime = System.currentTimeMillis();

        Dataset dataset = createDataset(dbName);
        stopTime = System.currentTimeMillis();
        evaluation.setExecuteTimeOrigin(stopTime - startTime);
        System.out.println("Loading DB :"+evaluation.toString());

        startTime = System.currentTimeMillis();
        // removing non-anonymized QID values
        updateQuery(dataset,"prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE { ?x ub:age ?a.}  WHERE { ?x ub:age ?a. FILTER(strlen(str(?a))<4) } ");
        updateQuery(dataset,"prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE { ?x ub:sex ?r.}  WHERE { ?x ub:sex ?r. } ");
        updateQuery(dataset,"prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE { ?x ub:zipcode ?z.}  WHERE {?x ub:zipcode ?z. FILTER(!STRENDS(?z,\"*\")) } ");

        // creating BN for professors
        updateQuery(dataset,"prefix owl:   <http://www.w3.org/2002/07/owl#> prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> INSERT { ?x owl:sameAs []} WHERE { ?x rdf:type ub:AssistantProfessor. } ");
        updateQuery(dataset,"prefix owl:   <http://www.w3.org/2002/07/owl#> prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> INSERT { ?x owl:sameAs []} WHERE { ?x rdf:type ub:FullProfessor.} ");
        updateQuery(dataset,"prefix owl:   <http://www.w3.org/2002/07/owl#> prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> INSERT { ?x owl:sameAs []} WHERE { ?x rdf:type ub:AssociateProfessor.} ");
            // Removing IDs of professors.
        updateQuery(dataset,"prefix owl:   <http://www.w3.org/2002/07/owl#> prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE { ?x ?p ?o. ?x owl:sameAs ?bn. ?bn owl:sameAs ?bn. } INSERT { ?bn ?p ?o.} WHERE { ?x owl:sameAs ?bn. ?x ?p ?o.} ");

        stopTime = System.currentTimeMillis();
        evaluation.setExecuteTimeOrigin(stopTime - startTime);
        System.out.println("Cleaning : "+evaluation.toString());
        dataset.begin(ReadWrite.READ);
        System.out.println("empty ? "+dataset.isEmpty() );
        dataset.commit();
        dataset.close();
    }
    public static void partition(String dbName)  throws IOException {
        startTime = System.currentTimeMillis();

        Dataset dataset = createDataset(dbName);
        stopTime = System.currentTimeMillis();
        evaluation.setExecuteTimeOrigin(stopTime - startTime);
        System.out.println("Loading DB :"+evaluation.toString());

        startTime = System.currentTimeMillis();
        // Moving QIDs to private, with blank node addition with age property
        updateQuery(dataset,"prefix owl:   <http://www.w3.org/2002/07/owl#> prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE  {?x ub:age ?a.} INSERT { GRAPH <http://private> { ?x ub:age ?a; owl:sameAs []. } } WHERE { ?x ub:age ?a. FILTER(strlen(str(?a))<4) } ");
        updateQuery(dataset,"prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE { ?x ub:sex ?r.} INSERT { GRAPH <http://private> { ?x ub:sex ?r. } } WHERE { ?x ub:sex ?r. } ");
        updateQuery(dataset,"prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE {?x ub:zipcode ?z.} INSERT { GRAPH <http://private> { ?x ub:zipcode ?z. } } WHERE { ?x ub:zipcode ?z. FILTER(!STRENDS(?z,\"*\")) } ");
         // Adding blank nodes in public to IDs
        updateQuery(dataset, "prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> prefix owl:   <http://www.w3.org/2002/07/owl#> prefix ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#> DELETE { ?x ?p ?o.} INSERT { ?bn ?p ?o } WHERE { ?x ?p ?o. GRAPH <http://private> {?x owl:sameAs ?bn }} ");

        stopTime = System.currentTimeMillis();
        evaluation.setExecuteTimeOrigin(stopTime - startTime);
        System.out.println("Cleaning & partitioning "+evaluation.toString());

        dataset.close();
    }
    public static void updateQuery(Dataset dataset, String query) {
        dataset.begin(ReadWrite.WRITE);
        UpdateRequest updateRequest = UpdateFactory.create(query);
        UpdateProcessor updateProcessor = UpdateExecutionFactory.create(updateRequest, dataset);
        updateProcessor.execute();
        dataset.commit();
    }

    public static Dataset createDataset(String dbName)  throws IOException {
        Dataset dataset = TDB2Factory.connectDataset(dbName);
        System.out.println("TDB2 ?"+ TDB2Factory.isTDB2(dataset));
        return dataset;
    }
}
