 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.kinesis.neo4j

import org.json4s.JObject
import org.json4s.JsonAST.JString
import org.slf4j.LoggerFactory

trait Logging {
  val LOG = LoggerFactory.getLogger(this.getClass)
}

/**
 * Format in which Snowplow events are buffered
 *
 * @param json The JSON string for the event
 * @param id The event ID (if it exists)
 */
case class JsonRecord(json: String, id: Option[String], jObject: Option[JObject] = None) {
  override def toString = json
}

case class BadJsonRecord(json: String)
case class Neo4jObject(index: String,  evt: String, source: Either[String, JObject], id: Option[String] = None) {
  def getSource = source

  def toRelationship: Option[(String, String, String)] = {
    source.right.toOption.flatMap { json =>

      val evt = json \\ "eventSource" match {
        case JString(s) => Some(s)
        case _ => None
      }
      val from = json \\ "from" \ "userid" match {
        case JString(s) => Some(s)
        case _ => None
      }
      val to = json \\ "to" \ "userid" match {
        case JString(s) => Some(s)
        case _ => None
      }

      println(s"$from - $evt - $to")

      for {
        e <- evt
        f <- from
        t <- to
      } yield (e, f, t)
    }
  }
}
