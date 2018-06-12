import utils.Parser

object Main {

  def main(args: Array[String]) : Unit = {

    Parser.readFile("dataset/friendships.dat")
  }
}
