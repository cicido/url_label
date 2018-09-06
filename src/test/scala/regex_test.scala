import scala.util.matching.Regex
object regex_test {
    def main(args: Array[String]) {
      val pattern = "(S|s)cala".r
      val str = "Scala is Scalable and cool"

      println(pattern.findFirstIn(str).isEmpty)
    }
}
