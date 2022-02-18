package query;

import algorithm.Tuple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import util.Counter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class QueryUtil {




    private static String fullProfessorsQueryString =
            "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>" +
                    "SELECT ?s " +
                    "WHERE {" +
                    "?s  a  ub:FullProfessor." +
                    "}";


    private static String associateProfessorsQueryString =
            "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>" +
                    "SELECT ?s " +
                    "WHERE {" +
                    "?s  a  ub:AssociateProfessor." +
                    "}";

    private static String assistantProfessorsQueryString =
            "PREFIX ub: <http://swat.cse.lehigh.edu/onto/univ-bench.owl#>" +
                    "SELECT ?s " +
                    "WHERE {" +
                    "?s  a  ub:AssistantProfessor." +
                    "}";


    public static List<QuerySolution> execQuery(Query query, Model model) {
        List<QuerySolution> solutionsList = new ArrayList<>();
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect();
            for (; results.hasNext(); ) {
                QuerySolution soln = results.nextSolution();
                solutionsList.add(soln);
            }
        }
        return solutionsList;
    }


    public static List<String> extractFullProfessorsFromGraph(Model model) {
        Query query = QueryFactory.create(fullProfessorsQueryString);
        List<QuerySolution> querySolutions = execQuery(query, model);
        return querySolutions.stream()
                .map(qs -> qs.get("s").toString())
                .collect(Collectors.toList());
    }


    public static List<String> extractAssistantProfessorsFromGraph(Model model) {
        Query query = QueryFactory.create(assistantProfessorsQueryString);
        List<QuerySolution> querySolutions = execQuery(query, model);
        return querySolutions.stream()
                .map(qs -> qs.get("s").toString())
                .collect(Collectors.toList());
    }


    public static List<String> extractAssociateProfessorsFromGraph(Model model) {
        Query query = QueryFactory.create(associateProfessorsQueryString);
        List<QuerySolution> querySolutions = execQuery(query, model);
        return querySolutions.stream()
                .map(qs -> qs.get("s").toString())
                .collect(Collectors.toList());
    }




    public static String createSelectClause(List<String> predicates) {
        StringBuilder selectClause = new StringBuilder("SELECT ?s ");
        int i;
        for (i = 0; i < predicates.size(); i++) {
            String attributeVariable = "?attr" + i;
            selectClause.append(attributeVariable).append(" ");
        }
        selectClause.append("\n");
        return selectClause.toString();
    }

    public static String createWhereClause(List<String> predicates) {
        StringBuilder whereClause = new StringBuilder("WHERE { ");
        for (int i = 0; i < predicates.size(); i++) {
            String attributeVariable = "?attr" + i;
            whereClause.append("?s ").append(predicates.get(i)).append(" ").append(attributeVariable).append(" . ");
        }
        return whereClause.toString();
    }


    public static String createQuery(List<String> predicates) {
        String selectClause = createSelectClause(predicates);
        StringBuilder whereClause = new StringBuilder(createWhereClause(predicates));
        whereClause.append("} \n");
        return selectClause + whereClause.toString();
    }



    public static List<List<String>> retrieveResultsFromQuery(List<String> predicates, Model model) {
        //System.out.println(createQuery(predicates));
        Query q = QueryFactory.create(createQuery(predicates));
        List<QuerySolution> sols = execQuery(q, model);

        List<List<String>> results = sols.stream().map(qs -> {
            List<String> lst = new ArrayList<>();
            lst.add(qs.get("s").toString());
            for (int i = 0; i < predicates.size(); i++) {
                String attributeVariable = "attr" + i;
                String[] tab = qs.get(attributeVariable).toString().split("\\^\\^");
                lst.add(tab[0]);
            }
            return lst;
        }).collect(Collectors.toList());

        return results;
    }

    public static void flushResultsToCsv(List<List<String>> results, List<String> columnNames, String filepath) throws IOException {
        Path p = Paths.get(filepath);
        StringBuilder sb = new StringBuilder();
        String columnNamesLine = columnNames.stream().collect(Collectors.joining(";", "", "\n"));
        sb.append(columnNamesLine);
        for (List<String> l : results) {
            String line = l.stream().collect(Collectors.joining(";", "", "\n"));
            sb.append(line);
        }
        Files.write(p, sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }


    public static String createDeleteClause(List<String> predicates, String sensitivePredicate, List<Boolean> isNumerical) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < predicates.size(); i++) {
            sb.append("?s ").append(predicates.get(i)).append(" ?attr").append(i).append(".\n");

            if (isNumerical.get(i)) {
                sb.append("?s ").append(predicates.get(i)).append(" ?o .\n")
                        .append("?o <http://minValue> ?min .\n")
                        .append("?o <http://maxValue> ?max .\n");
            }

        }
        sb.append("?s ").append(sensitivePredicate).append(" ?sa .");
        return sb.toString();
    }


    public static String createBlankNodeIntervalTriples(String tab[], String subject, String predicate, Counter blankIdCounter) {
         String s =  "<http://blank" + blankIdCounter.getValue() + "> .\n" +
                "<http://blank" + blankIdCounter.getValue() + ">  <http://minValue> " + tab[0] + " .\n" +
                "<http://blank" + blankIdCounter.getValue() + ">  <http://maxValue> " + tab[1] + " .\n";


        return s;
    }


    public static String createInsertClause(Tuple tuple, List<String> transformedData, List<String> predicates, String sensitivePredicate,
                                            List<Boolean> isNumerical, Counter blankIdCounter, Counter classIdCounter) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < predicates.size(); i++) {
            sb.append("<").append(transformedData.get(0)).append("> ").append(predicates.get(i)).append(" ");

            String[] tab = transformedData.get(i + 1).split("-");
            if (tab.length > 1) {
                sb.append(createBlankNodeIntervalTriples(tab, transformedData.get(0), predicates.get(i), blankIdCounter));
                blankIdCounter.increment();
            }
            else {
                if (isNumerical.get(i) && !tab[0].equals("*")) {
                    sb.append(tab[0]).append(" .\n");
                }
                else {
                    sb.append("\"").append(tab[0]).append("\" .\n");
                }
            }
        }
        sb.append("<").append(transformedData.get(0)).append("> ").append(sensitivePredicate).append(" \"").append(tuple.getSa()).append("\" .\n");
        return sb.toString();
    }

/*
    public static String createUpdateWhereClause(List<String> originalData, List<String> predicates, List<Boolean> isNumerical) {
        StringBuilder stringFirstClause = new StringBuilder("{ ");
        StringBuilder stringSecondClause = new StringBuilder("{ ");


        for (int i = 0; i < predicates.size(); i++) {
            stringFirstClause.append("<").append(originalData.get(0)).append("> ").append(predicates.get(i)).append(" ?attr").append(i).append(".\n");

            if (isNumerical.get(i)) {
                stringSecondClause.append("<").append(originalData.get(0)).append("> ").append(predicates.get(i)).append("  ?o .\n")
                        .append("?o <http://minValue> ?min .\n")
                        .append("?o <http://maxValue> ?max .\n");
            }
            else {
                stringSecondClause.append("<").append(originalData.get(0)).append("> ").append(predicates.get(i)).append(" ?attr").append(i).append(".\n");
            }

        }

        //stringFirstClause.append("<").append(originalData.get(0)).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> ?classId . \n}");
        //stringSecondClause.append("<").append(originalData.get(0)).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> ?classId . \n}");
        stringFirstClause.append("}");
        stringSecondClause.append("}");
        return stringFirstClause + " UNION \n" + stringSecondClause;
    }
*/


    public static String createUpdateWhereClause(List<String> predicates, String sensitivePredicate, List<Boolean> isNumerical) {
        StringBuilder stringFirstClause = new StringBuilder("{ ");
        StringBuilder stringSecondClause = new StringBuilder("{ ");


        for (int i = 0; i < predicates.size(); i++) {
            stringFirstClause.append("?s ").append(predicates.get(i)).append(" ?attr").append(i).append(".\n");

            if (isNumerical.get(i)) {
                stringSecondClause.append("?s ").append(predicates.get(i)).append("  ?o .\n")
                        .append("?o <http://minValue> ?min .\n")
                        .append("?o <http://maxValue> ?max .\n");
            }
            else {
                stringSecondClause.append("?s ").append(predicates.get(i)).append(" ?attr").append(i).append(".\n");
            }

        }

        stringFirstClause.append("?s ").append(sensitivePredicate).append(" ?sa .}");
        stringSecondClause.append("?s ").append(sensitivePredicate).append(" ?sa .}");
        return stringFirstClause + " UNION \n" + stringSecondClause;
    }




    public static List<List<String>> computeGeneralizationQueries(List<Tuple> tuples, List<List<String>> transformedData, List<String> predicates, String sensitivePredicate,
                                                                  List<Boolean> isNumerical, Counter blankIdCounter, Counter classIdCounter) {

        List<List<String>> updateQueries = new ArrayList<>();
        List<String> queriesPrivate = new ArrayList<>();
        List<String> queriesDefault = new ArrayList<>();

        for (int i = 0; i < transformedData.size(); i++) {
            //List<String> od = originalData.get(i);
            List<String> td = transformedData.get(i);

            // Update default
            String insertClause =  createInsertClause(tuples.get(i), td, predicates, sensitivePredicate, isNumerical, blankIdCounter, classIdCounter);
            String queryString = "INSERT DATA { \n" + insertClause + " }\n";
            queriesDefault.add(queryString);

            // Update privates
            if (!tuples.get(i).getIsFake()) {
                queryString = createUpdateQueryClassId(classIdCounter, td);
                queriesPrivate.add(queryString);
            }
            else {
                queriesPrivate.add(createInsertQueryPrivateFakeTuples(tuples.get(i), classIdCounter, sensitivePredicate));
            }
        }

        updateQueries.add(queriesPrivate);
        updateQueries.add(queriesDefault);
        return updateQueries;
    }



    private static String createUpdateQueryClassId(Counter classIdCounter, List<String> td) {
        String s = "DELETE { <" + td.get(0) + "> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> ?classId }\nINSERT { <"
                + td.get(0) + "> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> " + classIdCounter.getValue() + "}\nWHERE { <" + td.get(0)
                + "> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> ?classId }";
        return s;
    }


    private static String createInsertQueryPrivateFakeTuples(Tuple t, Counter classIdCounter, String sensitivePredicate) {
        StringBuilder sb = new StringBuilder("INSERT DATA {\n");
        sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ").append(t.getAge()).append(" .\n");
        sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ").append(t.getZipcode()).append(" .\n");
        sb.append("<").append(t.getId()).append("> ").append(sensitivePredicate).append(" \"").append(t.getSa()).append("\" .\n");
        sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#classID> ").append(classIdCounter.getValue()).append(" .\n");
        sb.append("<").append(t.getId()).append("> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 1").append(" .\n}");

        return sb.toString();
    }




    public static String createSelectClauseCountQuery(List<String> predicates) {
        StringBuilder selectClause = new StringBuilder("SELECT ");
        for (int i = 0; i < predicates.size(); i++) {
            String attributeVariable = "?attr" + i;
            selectClause.append(attributeVariable).append(" ");
        }
        selectClause.append("(COUNT(?s) as ?c) \n");
        return selectClause.toString();
    }




    public static String createGroupByClause(List<String> predicates) {
        StringBuilder groupByClause = new StringBuilder("GROUP BY  ");
        for (int i = 0; i < predicates.size(); i++) {
            String attributeVariable = "?attr" + i;
            groupByClause.append(attributeVariable).append(" ");
        }
        groupByClause.append("ORDER BY ?c");
        return groupByClause.toString();
    }


}
