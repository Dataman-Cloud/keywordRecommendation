package com.dataman.nlp.demo

/**
 * Created by fchen on 15-7-6.
 */
object Write {
  def main(args: Array[String]) = {
    var out = new java.io.FileWriter("/tmp/out.txt")
    out.write("中文")
    out close
  }
}
