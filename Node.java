import java.util.*;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

public class Node {

  public static enum Color {
    WHITE, GRAY, BLACK
  };

  private final int id;
  private int distance;
  private int currentDistance =Integer.MAX_VALUE ;	
  private List<Integer> edges = new ArrayList<Integer>();
  private List<Integer> weights = new ArrayList<Integer>();
  private List<Integer> route = new ArrayList<Integer>();
  private Color color = Color.WHITE;
  private int change;

  public Node(String str) {

    String[] map = str.split("\\s+");
    String key = map[0];
    String value = map[1];

    String[] tokens = value.split("\\|");
    this.id = Integer.parseInt(key);
    
     for (String s : tokens[0].split(",")) {
      if (s.length() > 0) {
        edges.add(Integer.parseInt(s));
      }
    }


    for (String s : tokens[1].split(",")) {
      if (s.length() > 0) {
        weights.add(Integer.parseInt(s));
      }
    }
    

    if (tokens[2].equals("Integer.MAX_VALUE")) {
      this.distance = Integer.MAX_VALUE;
    } else {
      this.distance = Integer.parseInt(tokens[2]);
      this.currentDistance = Integer.parseInt(tokens[2]);	
    }
   if(tokens[3].equals("NULL")){
        this.route = null;
    }else{
        for (String s : tokens[3].split(",")) {
          if (s.length() > 0) {
            route.add(Integer.parseInt(s));
          }
        }
    }

    this.color = Color.valueOf(tokens[4]);

  }

  public Node(int id) {
    this.id = id;
  }

  public int getId() {
    return this.id;
  }

  public int getDistance() {
    return this.distance;
  }

  public void setDistance(int distance) {
    this.distance = distance;
  }


  public int getCurrentDistance() {
    return this.currentDistance;
  }

  public void setCurrentDistance(int distance) {
    this.currentDistance = distance;
  }

  public Color getColor() {
    return this.color;
  }

  public void setColor(Color color) {
    this.color = color;
  }

  public List<Integer> getEdges() {
    return this.edges;
  }

  public void setEdges(List<Integer> edges) {
    this.edges = edges;
  }

  public List<Integer> getWeights() {
    return this.weights;
  }

  public void setWeights(List<Integer> weights) {
    this.weights = weights;
  }
  
  public List<Integer> getRoute() {
    return this.route;
  }

  public void setRoute(List<Integer> route) {
    this.route = route;
  }
  
  public Text getLine() {
    StringBuffer s = new StringBuffer();
    
    for (int v : edges) {
        s.append(v);
        if(v == this.edges.get(this.edges.size() - 1)){
            break;
        }
        s.append(",");
    }
    s.append("|");

    for (int v : weights) {
        s.append(v);
        if(v == this.weights.get(this.weights.size() - 1)){
            break;
        }
        s.append(",");
    }
    s.append("|");

    if (this.distance < Integer.MAX_VALUE) {
      s.append(this.distance).append("|");
    } else {
      s.append("Integer.MAX_VALUE").append("|");
    }
    
    if(this.route == null || this.route.isEmpty()){
       s.append("NULL");
    }else{
        for (int v : route) {
            s.append(v);
            if(v == this.route.get(this.route.size() - 1)){
                break;
            }
            s.append(",");
        }

    }
    s.append("|");
    

    s.append(color.toString());
	
    return new Text(s.toString());
  }

}
