package com.wise

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.text.SimpleDateFormat
import java.util.Date

package object utils{

  val timestampFormat: String = "yyyyMMddHHmmssSSS"
  val yearMonthDayFormat: String = "yyyyMMdd"

  implicit class Coalescer[A](val a: A) extends AnyVal {
    def ??(default: A): A = { Option(a).getOrElse(default) }
  }

  implicit class CoalescerEmpty[A](val a: String) extends AnyVal {
    def ??(default: String): String = { Option(a).filterNot(_.isEmpty).getOrElse(default) }
  }

  def formatDate(value: Date, pattern: String = yearMonthDayFormat): String = {
    new SimpleDateFormat(pattern).format(value)
  }

  def formatDateToTimestamp(value: Date): String = {
    formatDate(value, timestampFormat)
  }

  def parseDate(value: String, pattern: String = "dd/MM/yyyy"): Date = {
    new SimpleDateFormat(pattern).parse(value)
  }

  def serializer(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def deserializer[T](bytes: Array[Byte]): T = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject.asInstanceOf[T]
    ois.close
    value
  }
}
