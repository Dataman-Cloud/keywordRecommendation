package com.dataman.nlp.demo

import org.jsoup.Jsoup

/**
 * Created by fchen on 15-7-1.
 */
object JsoupDemo {
  def main(args: Array[String]): Unit = {
    val html = "< p >  这 两 家 机构 都 在 用 他们 自己 的 方式 努力 打造 一 个 不仅 是     大学 入学 考试 的 考试 。 ACT 计划 从 三 年级 起 就 每 年 测试 一 次 ， 以 帮助 学生们 为 大学 学习 作 准备 。 科尔曼 的 目标 之一 就 是 让 美国 大学     理事会 帮助 来自 低收入 家庭 的 学生们 看到 更 广阔 的 大学 入学 前景 。 < p >  < p >  < p > < img class = \" lazy_image \"  href = \" http : //s1.cdn.iw    eekly.bbwc.cn/filehub/img/201309 / 1379496073 qfc.jpg \"  onclick = \" javascript : iosuri ( ' slate : //gallery/http : //s1.cdn.iweekly.bbwc.cn/filehub    /img/201309 / 1379496072 jmj.jpg , http : //s1.cdn.iweekly.bbwc.cn/filehub/img/201309 / 1379496073 qfc.jpg , http : //s1.cdn.iweekly.bbwc.cn/filehub/i    mg/201309 / 1379496072 cxy.jpg , http : //s1.cdn.iweekly.bbwc.cn/filehub/img/201309 / 1379496073pys.jpg , http : //s1.cdn.iweekly.bbwc.cn/filehub/img/    201309 / 1379496072 asc.jpg' ) \"   >"
    val doc = Jsoup.parse(html.replaceAll(" ", ""))
    println(doc.text())
  }
}
