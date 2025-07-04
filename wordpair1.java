

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User:
 */
public class wordpair implements Writable,WritableComparable<wordpair> {

    private Text word;
    private Text fileName;

    public wordpair(Text word, Text fileName) {
        this.word = word;
        this.fileName = fileName;
    }

    public wordpair(String word, String fileName) {
        this(new Text(word),new Text(fileName));
    }

    public wordpair() {
        this.word = new Text();
        this.fileName = new Text();
    }

    @Override
    public int compareTo(wordpair other) {                         // A compareTo B
        int returnVal = this.word.compareTo(other.getWord());      // return -1: A < B
        if(returnVal != 0){                                        // return 0: A = B
            return returnVal;                                      // return 1: A > B
        }
        if(this.fileName.toString().equals("*")){
            return -1;
        }else if(other.getfileName().toString().equals("*")){
            return 1;
        }
        return this.fileName.compareTo(other.getfileName());
    }

    public static wordpair read(DataInput in) throws IOException {
        wordpair wordPair = new wordpair();
        wordPair.readFields(in);
        return wordPair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        fileName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word.readFields(in);
        fileName.readFields(in);
    }

    @Override
    public String toString() {
        return "{word=["+word+"]"+
               " fileName=["+fileName+"]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        wordpair wordPair = (wordpair) o;

        if (fileName != null ? !fileName.equals(wordPair.fileName) : wordPair.fileName != null) return false;
        if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
		int result = (word != null) ? word.hashCode() : 0;
        return word.hashCode();
    }

    public void setWord(String word){
        this.word.set(word);
    }
    public void setfileName(String fileName){
        this.fileName.set(fileName);
    }

    public Text getWord() {
        return word;
    }

    public Text getfileName() {
        return fileName;
    }
}