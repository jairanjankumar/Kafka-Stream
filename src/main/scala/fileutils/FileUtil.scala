package fileutils

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object FileUtil {

  def getListOfFiles(dir: String): List[File] = {

    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  def moveFiles(source: String, destination: String) = {

    Files.move(
      Paths.get(source),
      Paths.get(destination),
      StandardCopyOption.REPLACE_EXISTING
    )
  }

}
