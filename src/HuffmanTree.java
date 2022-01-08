import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

public class HuffmanTree {
    // Wurzelnode
    Node root;

    public HuffmanTree(Iterable<Text> values){
//      Speicher aller sortierter Nodes
        PriorityQueue<Node> nodes = new PriorityQueue<Node>();
//      Für jeden Buchstabe wird eine Node erstellt und in die Queue eingefügt
        for (Text valueText: values) {
            String value = valueText.toString();
            String[] valueParsed = value.split(":");
            String letter = valueParsed[0];
            String frequency = valueParsed[1];
            nodes.add(new Node(letter, Integer.parseInt(frequency)));
        }
//        Zusammenstellen des Baumes
//        Die Queue Size kann dabei nie 0 werden, da aus 2 Nodes eine wird
        while (nodes.size() > 1){
            Node left = nodes.remove();
            Node right = nodes.remove();
            nodes.add(new Node(left,right));
        }
        this.root = nodes.remove();
    }
//    rekursive Suchfunktion
    private void search(Node node, ArrayList<String> path, HashMap<String, String> paths){
//      Von Array lists lässt sich keine Deep Copy anfertigen
//      Daher wird in jeden durchlauf eine neue erstellt
        ArrayList<String> leftPath = new ArrayList<String>(path);
        ArrayList<String> rightPath = new ArrayList<String>(path);
//      abbrechen wenn es ein Blatt ist
        if (node.isLeaf()){
            path.remove(0);
            path.add(node.dir);
            paths.put(node.character, String.join("",path));
            System.out.println(node.character);
        }
        else{
//          anfügen der richtung
            leftPath.add(node.dir);
            rightPath.add(node.dir);
//          Rekursiver aufruf der funktion für die nächsten Nodes
            search(node.left, leftPath, paths);
            search(node.right, rightPath, paths);
        }
    }
//  Funktion für Initialen Aufruf der suchfunktion
    public HashMap<String, String>mapTree(){
        HashMap<String,String> map = new HashMap<String, String>();
        search(this.root, new ArrayList<String>(), map);
        return map;
    }
//  Nodes für den Baum
    public static class Node implements Comparable<Node>{
        public String character;
        public int frequency;
        public Node left;
        public Node right;
        public String dir;

//      Constructor für Blätter
        public Node(String character, int frequency) {
            this.character = character;
            this.frequency = frequency;
        }
//      Constructor für Nodes im Baum
        public Node(Node left, Node right) {
            this.left = left;
            left.dir = "0";
            this.right = right;
            right.dir = "1";
            this.frequency = left.frequency + right.frequency;
            System.out.println(this.frequency);
        }

        public boolean isLeaf(){
            return character != null;
        }

//      Vergleichsfunktion für PriorityQueue
        @Override
        public int compareTo(Node o) {
            return Integer.compare(this.frequency, o.frequency);
        }

    }

}
