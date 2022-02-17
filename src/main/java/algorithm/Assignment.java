package algorithm;

import java.util.*;
import java.util.stream.Collectors;

public class Assignment {


    public static List<Map.Entry<String, Integer>> computeListAttributesSize(Map<String, Integer> mapAttributesSize) {
        //List<Map.Entry<String, Integer>> l = new ArrayList<>(mapAttributesSize.entrySet());
        //l.sort(Map.Entry.comparingByValue());
        return new ArrayList<>(mapAttributesSize.entrySet());
    }


    public static int updateAttributeSize(Map.Entry<String, Integer> attributeSize, int alpha) {
        int oldValue = attributeSize.getValue();
        attributeSize.setValue(oldValue - alpha);
        return oldValue - alpha;
    }



    public static int computeAlpha(int m, int totalCount, int maxAlpha, int beta, List<Map.Entry<String, Integer>> listAttributesSize) {

        int alpha;
        for (alpha = maxAlpha; alpha > 0; alpha--) {
            //double member1 = (listAttributesSize.get(0).getValue() - alpha) * 1.0 / (totalCount * 1.0);
            double member1 = (listAttributesSize.get(0).getValue() - alpha) * 1.0;
            double member2 = (totalCount - (alpha * beta)) * 1.0 / m * 1.0;

            //System.out.println("MEMBER 1 : " + member1);
            //System.out.println("MEMBER 2 : " + member2);

            boolean inequality1 =  member1 <= member2;

            boolean inequality2;
            if (beta >= listAttributesSize.size()) {
                inequality2 = true;
            }
            else {
                //double member3 = (listAttributesSize.get(beta).getValue() - alpha) * 1.0 / (totalCount * 1.0);
                double member3 = listAttributesSize.get(beta).getValue() * 1.0;
                //System.out.println("MEMBER 3 : " + member3);
                inequality2 = member3 <= member2;
            }


            if (inequality1 && inequality2) {
                break;
            }
        }
        return alpha;
    }


    public static void assignment(int m, Map<String, List<Tuple>> mapNewTuples, Map<String, Integer> mapAttributesSize,
                                  Map<Set<String>, Bucket> mapBuckets) {

        Random r = new Random();
        boolean newBucket = false;
        int totalCount = mapAttributesSize.values().stream().reduce(Integer::sum).get();

        while(!mapAttributesSize.isEmpty()) {
            List<Map.Entry<String, Integer>> listAttributesSize = new ArrayList<>(mapAttributesSize.entrySet());
            //System.out.println("LIST   " + listAttributesSize);
            //List<Map.Entry<String, Integer>> toBeRemoved = new ArrayList<>();
            listAttributesSize.sort(Map.Entry.comparingByValue());
            Collections.reverse(listAttributesSize);
            //System.out.println(listAttributesSize.stream().map(Map.Entry::getValue).collect(Collectors.toList()));

            int beta = m;
            int alpha = computeAlpha(m, totalCount, listAttributesSize. get(beta - 1).getValue(), beta, listAttributesSize);
            while (alpha == 0) {
                beta++;
                alpha = computeAlpha(m, totalCount, listAttributesSize.get(beta - 1).getValue(), beta, listAttributesSize);
            }

            Set<String> signature = new HashSet<>();
            for (int i = 0; i < beta; i++) {
                Map.Entry<String, Integer> attribute = listAttributesSize.get(i);
                signature.add(attribute.getKey());
                //TODO supprimer
                //int newSize = updateAttributeSize(attribute, alpha);
                //if (newSize == 0) {
                //    toBeRemoved.add(attribute);
                //}
            }


            Bucket bucket = mapBuckets.get(signature);
            if (bucket == null) {
                bucket = new Bucket(signature);
                newBucket = true;
            }

            for (String attribute : signature) {
                int attributeSize = mapAttributesSize.get(attribute);
                List<Tuple> tuples = new ArrayList<>();
                for (int i = 0; i < alpha; i++) {
                    //int attributeSize = mapAttributesSize.get(attribute);
                    int index = r.nextInt(attributeSize);
                    Tuple t = mapNewTuples.get(attribute).remove(index);
                    tuples.add(t);
                    attributeSize--;
                }
                bucket.addTuples(tuples);


                /*
                listAttributesSize.removeAll(toBeRemoved);
                toBeRemoved.clear();
                */

                int finalAlpha = alpha;
                mapAttributesSize.computeIfPresent(attribute, (k, v) -> v - finalAlpha);
                if (mapAttributesSize.get(attribute) == 0) {
                    mapAttributesSize.remove(attribute);
                    //mapNewTuples.remove(attribute);
                    //listAttributesSize.remove()
                }

            }

            totalCount -= (alpha * beta);
            if (newBucket) {
                mapBuckets.put(signature, bucket);
            }
        }
    }


    public static void main(String[] args) {
        Map<Integer, List<Integer>> m = new HashMap<>();

        List<Integer> l1 = Arrays.asList(1,2,18,4,5);
        m.put(1, l1);
        System.out.println("Map ==> " + m);

        List<List<Integer>> l2 = new ArrayList<>();
        m.forEach((k,v) -> l2.add(v));
        System.out.println("L2 ==>  " + l2);

        List<Integer> l3 = l2.get(0);
        l3.add(24);
        System.out.println("L3 ==>  " + l3);
    }



}
