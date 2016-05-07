package main

import scala.util.control.Breaks._

/**
  * Created by Christos on 10/3/2016.
  */
class Graph {
  //variable that holds the range (always incremental)
  var range = 0
  //variable that holds the vertices of the graph
  private var vertices: Set[Long] = Set()
  //variable that increments every time a vertex is added (key of components map)
  private var id = 0
  //variable that holds the connected components of the graph
  private var components: Map[Int,Set[Long]] = Map()

  /**
    * Adds a vertex to the graph
    * @param vertexId vertex to add
    */
  def addVertex(vertexId: Long) = {
    if (range == 0) range = 1
    vertices += vertexId
    components += id -> Set(vertexId)
    id += 1
  }

  /**
    * Adds an edge to the graph
    * @param srcId
    * @param dstId
    */
  def addEdge(srcId: Long, dstId: Long) = {
    //variable that holds the id of the other end of the edge
    var valueAdded = 0L
    //variable that holds the key of a component
    var key1 = 0
    //variable that holds the key of the component to remove
    var key2 = 0
    //means it can break out of a loop
    breakable {
      //loop through connected components
      components.foreach{ case(k,v) =>
        //if a concatenation needs to be made
        if (valueAdded != 0L) {
          //and if current component contains the vertex we need to connect to
          if (v.contains(valueAdded)) {
            //hold the key of component to remove it later
            key2 = k
            //concatenate components
            val comps = components(key1) ++ v
            components += key1 -> comps
            //get the size of concatenated component
            val newRange = comps.size
            //if the size is greater than current range, replace and break the loop
            if (newRange > range) range = newRange
            break
          }
        }
        //if both ends of the edge are vertices within a component,
        //it means that the range will not change, so break
        if (v.contains(srcId) && v.contains(dstId)) break
        else if (v.contains(srcId)) {    //if srcId is contained in component, hold the key of the component and hold the vertex to connect
          key1 = k
          valueAdded = dstId
        }
        else if (v.contains(dstId)) {    //if dstId is contained in component, hold the key of the component and hold the vertex to connect
          key1 = k
          valueAdded = srcId
        }
      }
    }
    //if there was a concatenation of components, remove spare component
    if (valueAdded != 0L) {
      components -= key2
    }
  }

  /**
    * Checks whether a vertex exists in the graph
    * @param vertexId vertex to check
    * @return boolean
    */
  def vertexExists(vertexId: Long) = vertices.contains(vertexId)

  /**
    * Checks if the graph is empty
    * @return boolean
    */
  def isEmpty = vertices.isEmpty

  /**
    * Adds multiple edges in the graph
    * @param vertexId srcId of the edge
    * @param edges edges to add to the graph
    */
  def addEdgesToVertex(vertexId: Long, edges: Array[Long]) = {
    edges.foreach(addEdge(vertexId,_))
  }
}
