package ckite.mapdb

import java.io.File

trait FileSupport {

  protected def file(dataDir: String, fileName: String): File = {
    val dir = new File(dataDir)
    dir.mkdirs()
    val file = new File(dir, fileName)
    file
  }

}
