package com.snowplowanalytics.snowplow.storage.kinesis.neo4j

import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer
import scala.collection.JavaConversions._
import org.anormcypher._

object SnowplowNeo4jEmitterHelpers {
  private val events = Map(
    "like" -> "LIKED",
    "viewed" -> "VIEWED",
    "message" -> "MESSAGED"
  )

  def eventToNeo4jEvent(evt: String) = {
    events.get(evt)
  }
}

class SnowplowNeo4jEmitter extends IEmitter[Neo4jObject] with Logging {
  implicit val store = Neo4jREST

  def uidToNeoTypeName(in: String) = {
    s"""p_$in"""
  }

  def createNode(nodeName: String, nodeType: String = "Person") = {
    s"""(${uidToNeoTypeName(nodeName)}:Person { name:"$nodeName" })"""
  }

  def write(from: String, to: String, relationship: String) = {
    val mergeQuery = s"MERGE ${createNode(from)} MERGE ${createNode(to)} MERGE ${uidToNeoTypeName(from)}-[:$relationship]->${uidToNeoTypeName(to)};"
    mergeQuery
  }

  def emit(buffer: UnmodifiableBuffer[Neo4jObject]): java.util.List[Neo4jObject] = {
    val records:List[Neo4jObject] = buffer.getRecords.toList

    for(record <- records) yield {
      val relationship = record.toRelationship

      relationship.flatMap { relation =>
        val (evt, from, to) = relation
        val event = SnowplowNeo4jEmitterHelpers.eventToNeo4jEvent(evt)
        event.map(e => LOG.info(Cypher(write(from, to, e)).execute().toString))
      }

      record
    }
  }

  def fail(records:java.util.List[Neo4jObject]) {

  }

  def shutdown {

  }
}
