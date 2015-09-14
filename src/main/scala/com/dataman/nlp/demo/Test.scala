package com.dataman.nlp.demo

/**
 * Created by fchen on 15-9-10.
 */
object Test {
  def main(args: Array[String]) = {
    val str = """ < \/span > < \/h3 >  \r\n 白羊座 稳定 持续 好 状态 的 一 周 。"""
    val m = """\\r\\n|\\r|\\n|\\"""
    println(str.replaceAll(m, ""))

    println("""/taaa""".replaceAll("""/t""", ""))

    val a = """[ t ] 一 条 边境 ， 两 个 世界 [ \/t ]"""
    println(a.replaceAll("""\[ t \]|\[ \\/t \]""", ""))

    "a1b2"
    println("""[1-9,0]""".r.findAllMatchIn("25").isEmpty)
    println("""[a-z,A-Z]""".r.findAllMatchIn("123").isEmpty)
  }
}
