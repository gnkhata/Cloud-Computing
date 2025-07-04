import java.util.*;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

public class Node {

  public static enum Color {
    WHITE, GRAY, BLACK
  };

  private final int id;
  private float mass;
  private List<Integer> edges = new ArrayList<Integer>();
  private int change;

  public Node(String str) {

    String[] map = str.split("\\s+");
    String key = map[0];
    String value = map[1];

    String[] tokens = value.split("\\|");
    this.id = Integer.parseInt(key);
     if(tokens[0].equals("NULL")){
        this.edges = null;
     }else{
         for (String s : tokens[0].split(",")) {
          if (s.length() > 0) {
            edges.add(Integer.parseInt(s));
          }
         }
     }

    this.mass = Float.parseFloat(tokens[1]);

  }

  public Node(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public float getMass() {
    return this.mass;
  }

  public void setMass(float mass) {
    this.mass = mass;
  }


  public List<Integer> getEdges() {
    return this.edges;
  }

  public void setEdges(List<Integer> edges) {
    this.edges = edges;
  }

  


  public Text getLine() {
    StringBuffer s = new StringBuffer();
    
    if(this.edges == null || this.edges.isEmpty()){
       s.append("NULL");
    }else{
        for (int v : edges) {
            s.append(v);
            if(v == this.edges.get(this.edges.size() - 1)){
                break;
            }
            s.append(",");
        }

    }
    s.append("|");
    
    s.append(this.mass);
	
    return new Text(s.toString());
  }

}
