package kgastor.main;
import org.apache.jena.dboe.transaction.txn.TransactionException;
import org.apache.jena.query.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.TDB2Factory;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

import static org.apache.jena.rdf.model.impl.RDFDefaultErrorHandler.logger;

public class SelectQr {
    private String dbName;
    private String queryFile;
    public SelectQr(String dbName, String queryFile) {
        this.dbName = dbName;
        this.queryFile = queryFile;
    }
    public void query() {
        long startTime, stopTime;
        try {
            ArrayList<String> queries = queryLoader(queryFile);
            Dataset dataset = TDB2Factory.connectDataset(dbName);

            int cpt=0;
            for(String qr : queries) {
                startTime = System.currentTimeMillis();
                dataset.begin(ReadWrite.READ);
                System.out.println("in txn ?"+dataset.isInTransaction()+ " empty "+dataset.isEmpty());
                QueryExecution qe = QueryExecutionFactory.create(qr,dataset);
                for (ResultSet results = qe.execSelect(); results.hasNext();) {
                    QuerySolution qs = results.next();
                    Iterator<String> it = qs.varNames();
                    while(it.hasNext())
                        System.out.print(qs.get(it.next())+"\t");
                    System.out.println();
                }
                dataset.commit();

                System.out.println("duration of query #"+cpt+" = "+ (System.currentTimeMillis() - startTime)+ "ms");
                cpt++;
                System.out.println("===============================================================");
            }


        } catch (FileNotFoundException fnfe) {
            System.out.println("File not found exception "+fnfe.getMessage());
        } catch(TransactionException te) {
            System.out.println("Txn exception : "+te.getMessage());
        }


    }
    public ArrayList<String> queryLoader(String file) throws FileNotFoundException {
        ArrayList<String> queries = new ArrayList<String>();
        Scanner sc = new Scanner(new File(file));
        while (sc.hasNext()) {
            String tmp = sc.nextLine();
            if(!tmp.startsWith("//"))
                queries.add(tmp);
        }
        return queries;
    }
}
