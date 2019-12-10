/* CS441 Course Project: Chord Algorithm Akka/HTTP-based Simulator
 * Team:   Carlos Antonio McNulty,  cmcnul3 (Leader)
 *         Abram Gorgis,            agorgi2
 *         Priyan Sureshkumar,      psures5
 *         Shyam Patel,             spate54
 * Date:   Dec 10, 2019
 */

// Class to keep metadata for inserts and lookups
class FileMetadata(filename: String, size: Int){
  def getFilename: String ={
    filename
  }
  def getSize: Int ={
    size
  }
}
