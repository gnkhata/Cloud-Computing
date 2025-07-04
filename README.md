# Cloud-Computing
Class projects for cloud computing class at University of Arkansas Fall 2023. **Caution**: Each module works independently with its specified task explained except where explicitly stated.

**WordCount.java** performs word count, but also figures out how many times the map method is invoked in the map stage. it writes the number of times the map method is invoked to one output file. It Does NOT use any built-in counter or global counter for this purpose.

**BigramCount.java** counts bigrams (pairs of consecutive words) instead of individual words; delimit the two words in a bigram with space, – Example: for a line containing three words “A B C”, two bigrams are produced, i.e., “A B” and “B C”
* “A” and “B” can be concatenated to form a composite word “A B”, – If a line contains only one word, skip this line
The code also figue out how many map tasks are launched in the map stage
– A map task is a job to process one file split.
– it Does NOT use built-in counters, global variables, static variables. (This sample code reads three files)

**NewText.java** gives the output of reducers is in the reversed alphabetical order (different from the original input).
– NewText.java contains the java class defining the new “Text” data type.

**ReduceCallTimes.java** determines the number of times the reduce method is called in a reducer. It assumes that there is only one reducer in the reduce stage; It does not use any built-in counters; 
* A reducer is an instance of the reducer class.
* The number of times the reduce method is called needs to be saved into the output file only once as “–reduce method count– xyz”, where xyz is the number.

**relativefreq.java** works with **wordpair.java**  to calculate the relative frequencies of the co-occurrences of word pairs. Study the sample code from the
provided folder ComplexKeys.
* Modifies the hashCode() method in the wordpair class so that all word pairs sharing the same left word go to the same reduce task.
* Implements the reduce() method in the relativefreq.java to calculate the relative frequencies of word pairs

**invertedindex.java** works with **wordpair1.java** to generate an inverted index given a set of input files. The input contains a set of text files. Each file contains words without punctuation marks in multiple lines. Assume that the size of a file is less than 128 MB. The files are named as “file0”, “file1”, “file2”, ......
* The generated inverted index is distributively saved in multiple files. An output file contains multiple lines. Each line consists of a term (i.e., a word) and the posting list.
* In each output file, those lines are listed in an alphabetic order.
* Each posting list is in such a format as “file name: # of occurrence; file name: # of occurrence...”. The posting list needs to be in the order of file names. Example: “file0: 18; file1: 20; file2: 3;”.
* It specifies 3 reducers, e.g., job.setNumReduceTasks(3);
* The mapper uses pairs approach to generate complex keys, i.e., (term, filename), in the mapper stage. The value is the total number of occurrences the term appears in the file.

**GraphSearch.java** performs a parallel breadth-first search on a directed graph.

**Node.java** works with **GraphSearch1.java** to perform an alternative parallel breadth-first search on a directed graph.

**PageRank.java** works with **Node1.java** to perform a parallel PageRank algorithm on a directed graph.
