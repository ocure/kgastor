package evaluation;

import java.util.List;

import static query.QueryUtil.createGroupByClause;
import static query.QueryUtil.createSelectClauseCountQuery;

public class Queries {


    public static String createWhereClauseAgeQueryBeforeAnon(int minValue, int maxValue, List<String> predicates) {
        StringBuilder whereClause = new StringBuilder("WHERE { ");
        for (int i = 0; i < predicates.size(); i++) {
            String attributeVariable = "?attr" + i;
            whereClause.append("?s ").append(predicates.get(i)).append(" ").append(attributeVariable).append(" . ");
        }
        whereClause.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?o . ")
                .append("FILTER(?o >= ").append(minValue).append(" && ?o < ").append(maxValue).append(") . }\n");
        return whereClause.toString();
    }


    public static String createWhereClauseAgeQuery(int minValue, int maxValue, String saGroup) {
        StringBuilder whereClause = new StringBuilder("WHERE { { ");

        whereClause.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?o . ")
                .append("FILTER(?o >= ").append(minValue).append(" && ?o < ").append(maxValue).append(") . ");
        if (saGroup != null) {
            whereClause.append("?s <http://inGroup> <").append(saGroup).append("> . ");
        }

        whereClause.append(" } UNION { \n");

        whereClause.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?b . ")
                .append("?b <http://minValue> ?min . ")
                .append("?b <http://maxValue> ?max . ")
                .append("FILTER(?min >= ").append(minValue).append(" && ?max < ").append(maxValue).append(") . ");

        if (saGroup != null) {
            whereClause.append("?s <http://inGroup> <").append(saGroup).append("> . ");
        }

        whereClause.append("}}");
        return whereClause.toString();
    }


    public static String createCountAgeQueryBeforeAnon(int minValue, int maxValue, List<String> predicates) {
        StringBuilder queryStr = new StringBuilder();
        queryStr.append(createSelectClauseCountQuery(predicates));
        queryStr.append(createWhereClauseAgeQueryBeforeAnon(minValue, maxValue, predicates));
        if (!predicates.isEmpty()) {
            queryStr.append(createGroupByClause(predicates));
        }
        return queryStr.toString();
    }



    public static String createCountAgeQuery(int minValue, int maxValue, String saGroup) {
        String selectClause = "SELECT (COUNT(?s) as ?c) \n";
        String whereClause = createWhereClauseAgeQuery(minValue, maxValue, saGroup);

        return selectClause + whereClause;
    }


    public static String createWhereClauseAgeZipcodeQueryBeforeAnon(List<String> predicates, int minValue, int maxValue, String zipcode) {
        StringBuilder whereClause = new StringBuilder("WHERE { ");
        for (int i = 0; i < predicates.size(); i++) {
            String attributeVariable = "?attr" + i;
            whereClause.append("?s ").append(predicates.get(i)).append(" ").append(attributeVariable).append(" . ");
        }
        whereClause.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#age> ?o . ")
                .append("FILTER(?o >= ").append(minValue).append(" && ?o < ").append(maxValue).append(") . ");

        whereClause.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#zipcode> ?z . ")
                .append("FILTER regex(?z, \"^").append(zipcode).append("\") . ");

        whereClause.append("?s <http://swat.cse.lehigh.edu/onto/univ-bench.owl#tombstone> 0 . ");

        whereClause.append("} ");
        return whereClause.toString();
    }


    public static String createCountAgeZipcodeQueryBeforeAnon(List<String> predicates, int minValue, int maxValue, String zipcode) {
        StringBuilder queryStr = new StringBuilder();
        queryStr.append(createSelectClauseCountQuery(predicates));
        queryStr.append(createWhereClauseAgeZipcodeQueryBeforeAnon(predicates, minValue, maxValue, zipcode));
        if (!predicates.isEmpty()) {
            queryStr.append(createGroupByClause(predicates));
        }
        return queryStr.toString();
    }


}
