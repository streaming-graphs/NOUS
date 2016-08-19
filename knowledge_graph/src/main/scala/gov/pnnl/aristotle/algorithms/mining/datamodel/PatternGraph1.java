package gov.pnnl.aristotle.algorithms.mining.datamodel;
// Java program to print DFS traversal from a given given graph
import java.io.*;
import java.util.*;
import gov.pnnl.aristotle.algorithms.mining.datamodel.PatternTriple;
 
// This class represents a directed graph using adjacency list
// representation
class PatternGraph1
{
    private int V;   // No. of vertices
 
    // Array  of lists for Adjacency List Representation
    private LinkedList<PatternTriple> adj[];
 
    // Constructor
    PatternGraph1(int v)
    {
        V = v;
        adj = new LinkedList[v];
        for (int i=0; i<v; ++i)
            adj[i] = new LinkedList();
    }
 
    /**
		 * @param pattern
		 */
    public PatternGraph1(String pattern)
    {
	    // TODO Auto-generated constructor stub
    //	String[] pattern_array = 
    	
    }

		//Function to add an edge into the graph
    void addEdge(int v, int w)
    {
      //  adj[v].add(w);  // Add w to v's list.
    }
 
    // A function used by DFS
    void DFSUtil(int v,boolean visited[])
    {
        // Mark the current node as visited and print it
        visited[v] = true;
        System.out.print(v+" ");
 
        // Recur for all the vertices adjacent to this vertex
        Iterator<PatternTriple> i = adj[v].listIterator();
        while (i.hasNext())
        {
            int n = 1; //i.next();
            if (!visited[n])
                DFSUtil(n, visited);
        }
    }
 
    // The function to do DFS traversal. It uses recursive DFSUtil()
    void DFS(int v)
    {
        // Mark all the vertices as not visited(set as
        // false by default in java)
        boolean visited[] = new boolean[V];
 
        // Call the recursive helper function to print DFS traversal
        DFSUtil(v, visited);
    }
 
    public static void main(String args[])
    {
//    	PatternGraph g = new PatternGraph(4);
//    	String pattern = "";
//    	PatternGraph g1 = new PatternGraph(pattern)
//        g.addEdge(0, 1);
//        g.addEdge(0, 2);
//        g.addEdge(1, 2);
//        g.addEdge(2, 0);
//        g.addEdge(2, 3);
//        g.addEdge(3, 3);
 
        System.out.println("Following is Depth First Traversal "+
                           "(starting from vertex 2)");
 
        //g.DFS(2);
    }
}