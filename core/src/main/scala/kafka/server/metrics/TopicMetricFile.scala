/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.metrics

import org.apache.kafka.storage.internals.log.LogDirFailureChannel

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

@SerialVersionUID(100L)
case class TopicRecord(byte: Int, timestamp: Long) extends Serializable

@SerialVersionUID(100L)
class BrokerRecordByte(expirationMillis: Long, maxSize: Int) extends Serializable {
  val records: mutable.HashMap[String, mutable.ArrayDeque[TopicRecord]] = mutable.HashMap[String, mutable.ArrayDeque[TopicRecord]]()
  val byteSum: mutable.Map[String, Long] = mutable.HashMap[String, Long]().withDefaultValue(0L)

  def append(topic: String, byte: Int, timestamp: Long): Unit = {
    val deque = records.getOrElseUpdate(topic, mutable.Queue[TopicRecord]())

    cleanUpOldRecords(topic, timestamp)

    deque.append(TopicRecord(byte, timestamp))
    byteSum(topic) += byte

    if (maxSize > 0 && deque.size > maxSize) {
      val removed = deque.removeHead()
      byteSum(topic) -= removed.byte
    }
  }

  private def cleanUpOldRecords(topic: String, currentTimestamp: Long): Unit = {
    if (expirationMillis > 0) {
      records.get(topic).foreach { deque =>
        while (deque.nonEmpty && currentTimestamp - deque.head.timestamp > expirationMillis) {
          val removed = deque.removeHead()
          byteSum(topic) -= removed.byte
        }
      }
    }
  }

  def bytesPerSecond(topic: String): Float = {
    records.get(topic).map { deque =>
      if (deque.nonEmpty && deque.last.timestamp != deque.head.timestamp) {
        val timeDiff = deque.last.timestamp - deque.head.timestamp
        byteSum(topic).toFloat / timeDiff * 1000
      } else 0f
    }.getOrElse(0f)
  }
}

class TopicMetricFile(file: File, logDirFailureChannel: LogDirFailureChannel = null) {
  val metrics: BrokerRecordByte = loadMetricsFromFile()

  private def loadMetricsFromFile(): BrokerRecordByte = {
    if (file.exists()) {
      var brokerRecordByte: BrokerRecordByte = null
      val fileIn = new FileInputStream(file)
      val in = new ObjectInputStream(fileIn)
      try {
        brokerRecordByte = in.readObject().asInstanceOf[BrokerRecordByte]
      } catch {
        case ex: Exception =>
          println(s"Failed to read from file $file: ${ex.getMessage}")
          brokerRecordByte = new BrokerRecordByte(1000L * 60 * 60, -1)
      } finally {
        in.close()
        fileIn.close()
      }
      brokerRecordByte
    } else {
      new BrokerRecordByte(1000L * 60 * 60, -1)
    }
  }

  def putRecord(topic: String, byte: Int): Unit = {
    metrics.append(topic, byte, System.currentTimeMillis())
  }

  def write(): Unit = {
    val fileOut = new FileOutputStream(file)
    val out = new ObjectOutputStream(fileOut)
    try {
      out.writeObject(metrics)
    } catch {
      case ex: Exception =>
        println(s"Failed to write to file $file: ${ex.getMessage}")
        ex.printStackTrace()
    } finally {
      out.close()
      fileOut.close()
    }
  }
}