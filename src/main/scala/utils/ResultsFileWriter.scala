package utils

import java.io.{BufferedWriter, File, FileWriter}

object ResultsFileWriter {

  val outputDir: String = "results/"

  def writeLine(line: String, name: String) : Unit = {
    val file = new File(outputDir + name)

    if (!file.exists)
      file.createNewFile

    val bw = new BufferedWriter(new FileWriter(file, true))

    bw.append(line + "\n")

    bw.close()
  }
}
