package algorithm;

import java.util.*;
import java.util.stream.Collectors;

public class Bucket {

    private final Map<String, List<Tuple>> attributes ;


    public Bucket(Set<String> signature) {
        attributes = new HashMap<>();
        signature.forEach(attribute -> attributes.put(attribute, new ArrayList<>()));
    }


    public Map<String, List<Tuple>> getAttributes() {
        return attributes;
    }


    public List<List<Tuple>> getAttributesLists() {
        List<List<Tuple>> res = new ArrayList<>();
        attributes.forEach((k,v) -> res.add(v));
        return res;
    }

    public Set<String> getSignature() {
        return this.attributes.keySet();
    }

    public void addTuples(List<Tuple> tuples) {
        tuples.forEach(t -> {
            try {
                this.attributes.get(t.getSa()).add(t);
            }
            catch(NullPointerException e) {
                e.printStackTrace();
            }
        });
    }


    public List<String> getUnbalancedAttributes(int maxAttributeSize) {
        List<String> unBalancedattributes = attributes.entrySet().stream().filter(e -> e.getValue().size() < maxAttributeSize).
                map(Map.Entry::getKey).collect(Collectors.toList());

        return unBalancedattributes;
    }


    public int getMaxAttributeSize() {
        Optional<Integer> maxSize = attributes.entrySet().stream().map(e -> e.getValue().size()).max(Integer::compareTo);
        return maxSize.orElse(0);
    }


    public int size() {
        return attributes.entrySet().stream().map(e -> e.getValue().size()).reduce(0, Integer::sum);
    }


    public List<Tuple> getTuples() {
        return attributes.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }


    public List<Integer> getMinMaxAgeZip() {
        List<Integer> res = new ArrayList<>();
        int minAge = Integer.MAX_VALUE;
        int maxAge = Integer.MIN_VALUE;
        int minZip = Integer.MAX_VALUE;
        int maxZip = Integer.MIN_VALUE;

        for (Map.Entry<String, List<Tuple>> e : this.attributes.entrySet()) {
            List<Integer> localMinMax = Split.findMinMaxAgeZip(e.getValue());
            if (localMinMax.get(0) < minAge) {
                minAge = localMinMax.get(0);
            }
            if (localMinMax.get(1) > maxAge) {
                maxAge = localMinMax.get(1);
            }
            if (localMinMax.get(2) < minZip) {
                minZip = localMinMax.get(2);
            }
            if (localMinMax.get(3) > maxZip) {
                maxZip = localMinMax.get(3);
            }
        }
        res.add(minAge);
        res.add(maxAge);
        res.add(minZip);
        res.add(maxZip);

        return res;
    }



    public int balance(Map<String, List<Tuple>> mapNewTuples, int m, int indexFakeCounter) {

        Random r = new Random();

        int indexFake = indexFakeCounter;
        int maxAttributeSize = this.getMaxAttributeSize();
        List<String> unbalancedAttributes = this.getUnbalancedAttributes(maxAttributeSize);
        //TODO Déclarer en dehors attributeSize
        Map<String, Integer> mapAttributesSize = Balancing.computeMapSaSize(mapNewTuples);
        int totalCount = mapAttributesSize.values().stream().reduce(Integer::sum).get();
        AbstractMap.SimpleEntry<String, Integer> mostRepresented = Balancing.findMostRepresentedAttribute(mapAttributesSize);

        for (String attr : unbalancedAttributes) {
            int neededTuplesNb = maxAttributeSize - attributes.get(attr).size();
            int removedTuplesNb = neededTuplesNb;
            if (mapNewTuples.get(attr) != null && !mapNewTuples.get(attr).isEmpty()) {
                removedTuplesNb = Math.min(neededTuplesNb, mapNewTuples.get(attr).size());

                // As long as we can remove tuples and keep being m-eligible
                for (int i = 0; i < removedTuplesNb; i++) {

                    //Tuple t = mapNewTuples.get(attr).remove(index);
                    mapAttributesSize.computeIfPresent(attr, (k,v) -> v - 1);
                    if (attr.equals(mostRepresented.getKey())) {
                        mostRepresented = Balancing.findMostRepresentedAttribute(mapAttributesSize);
                    }

                    if (!Balancing.mEligibilityTest(mostRepresented.getValue(), totalCount - 1, m)) {
                        //mapNewTuples.get(attr).add(t);
                        mapAttributesSize.computeIfPresent(attr, (k,v) -> v + 1);
                        if (attr.equals(mostRepresented.getKey())) {
                            mostRepresented = Balancing.findMostRepresentedAttribute(mapAttributesSize);
                        }
                    }
                    else {
                        int index = r.nextInt(mapNewTuples.get(attr).size());
                        Tuple t = mapNewTuples.get(attr).remove(index);
                        this.attributes.get(attr).add(t);
                        neededTuplesNb--;
                        totalCount--;
                        // If list in newTuples is empty, the key can be removed
                        if (mapAttributesSize.get(attr) == 0) {
                            mapAttributesSize.remove(attr);
                            mapNewTuples.remove(attr);
                        }

                    }
                }
            }


            List<Integer> minMaxAgeZip = this.getMinMaxAgeZip();

            for (int i = 0; i < neededTuplesNb; i++) {
                // add fake tuples
                // TODO Créer un bon id pour les counterfeits
                this.attributes.get(attr).add(new Tuple("http://Fake" + indexFake, -1, minMaxAgeZip.get(0), minMaxAgeZip.get(2).toString(), attr, true, true));
                indexFake++;
            }
        }
        return indexFake;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        attributes.forEach((signature,tuples) -> {
            sb.append(signature).append("  ==> ");
            sb.append((tuples.stream().map(x -> "(" + x.getId() + ", " + x.getAge() + ", " + x.getZipcode() + ")")
                    .collect(Collectors.joining(",", "[", "]\n"))));
        });

        return sb.toString();
    }
}
