package com.wise

import java.io.{File, PrintWriter}

object GenerateFile {

  def main(args: Array[String]): Unit = {
    val file = new File("./generate/abc.csv")
    val printWriter = new PrintWriter(file)
    for (i <- 1000 to 1500) {
      for (j <- 10000 to 99999) {
        printWriter.write(i + ";" + j + "\n")
      }
    }
    printWriter.close()
  }

}
