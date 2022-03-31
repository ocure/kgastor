# Kgastor: a Privacy By Design Knowledge Graph anonymized Store

## Overview

This project's purpose is to provide our proposition for Kgastor which incorporates building blocks such as Jena, ARX and our implementation of the m-invariance algorithm.

The code provided allows the user to run the evaluations displayed in Section 6 of our paper.


## Execution    

A .jar file is provided for an easier execution in the executable directory. 

A number of options are provided, these can be separated into different 
categories depending on the part of the program to which they apply:

**Loading data** 
 
> -  -d [directory1] [directory2] ...  :   Loads the files contained in the
        directories into the model.
> - -f  [file1] [file2] …   :   Loads the files into the model.
> - -tdb [dataset]    :    Loads a Jena TDB dataset into the model.
  
**M-invariance**

> - -csv_path   [dataset.csv]   :   Provide path to the csv file used by ARX for the anonymization.
> - -m  [value] : Provide the value for the “m” parameter.
> - -zip_range [value] : Provide the range between the highest zip code and the lowest. This value is used in the split phase in order to normalize the domain of zip codes so that all QIDs are comparable.
> - -ratio [value] : Provide the number of releases to be stimulated. This directly influences the number of tuples removed and added between each release (when the rate grows, the number of updates between each release decreases).
> - -hierarchies [hierarchy1] [hierarchy2] …  :  Provide paths to csv files containing the generalization hierarchies for the QIDs.
> - -zipSuffix [N]  :  Sets the generalization level for the zipcodes (the size of the prefix) used when creating counting queries during the evaluation

**Example**

java -jar M-invarianceRdf.jar -tdb dataset/ -f  religion.ttl  zip.ttl   age.ttl    -hierarchies age_hierarchy.csv    zip_hierarchy_3000.csv -csv_path arx.csv -m 4 -zip_range 3000 -ratio 100 -sensitive "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#hasReligion>" -zipSuffix 3

**Datasets**
During our evaluation, we used our own extension of LUBM (see paper). Even the smallest of our dataset (LUBM100) is already too large to fit into our public github account. You can access both the original (lubm100.nt) and privacy-enriched LUBM 100 (lubmAno100.ttl) files at http://igm.univ-mlv.fr/~ocure/kgastor/
