package algorithm;

import java.util.*;

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





    private static double computeSchemesPerimeter(MinMaxAgeZip minMax, int bucketSize, int ageRange, int zipRange) {
        double agePerimeter = (minMax.maxAge - minMax.minAge) * 1.0 / ageRange;
        double zipPerimeter = (minMax.maxZip - minMax.minZip) * 1.0 / zipRange;

        return (agePerimeter + zipPerimeter) * bucketSize * 1.0;
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
            AbstractMap.SimpleEntry<Double, Integer> bestSchemeAge = findBestSplitScheme(attributesList, ageRange, zipRange);


            // Sort attributes lists by zip
            attributesList.forEach(l -> {
                l.sort(Comparator.comparing(Tuple::getZipcode));
            });
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

}
