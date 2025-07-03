# Cloud-Computing
Class projects for cloud computing class at University of Arkansas Fall 2023

**WordCount.java** performs word count, but also figures out how many times the map method is invoked in the map stage. it writes the number of times the map method is invoked to one output file. It Does NOT use any built-in counter or global counter for this purpose.

**BigramCount.java** counts bigrams (pairs of consecutive words) instead of individual words; delimit the two words in a bigram with space, – Example: for a line containing three words “A B C”, two bigrams are produced, i.e., “A B” and “B C”
* “A” and “B” can be concatenated to form a composite word “A B”, – If a line contains only one word, skip this line
The code also figue out how many map tasks are launched in the map stage
– A map task is a job to process one file split.
– it Does NOT use built-in counters, global variables, static variables. (This sample code reads three files)

**NewText.java** gives the output of reducers is in the reversed alphabetical order (different from the original input).
– NewText.java contains the java class defining the new “Text” data type.
