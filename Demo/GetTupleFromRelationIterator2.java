package Demo;

import StorageManager.Storage;
import PTCFramework.ProducerIterator;
import java.nio.ByteBuffer;

public class GetTupleFromRelationIterator2 implements ProducerIterator<byte[]>{
  int currentPage = 0;
  Storage storageObject;
  int nextpage = -1;
  int tuplelength;
  int tuplesProcessed = 0;
  int maximumTuples;
  byte[] currentBuffer;
  String fileName;
  int pageSize;

  public GetTupleFromRelationIterator2(String fileName, int lengthOfTuple, int initialPage) throws Exception{
    fileName = fileName;
    tuplelength = lengthOfTuple;
    storageObject = new Storage();
    storageObject.LoadStorage(fileName);
    pageSize = storageObject.pageSize;
    currentPage = initialPage;
  }

  public boolean hasNext(){
    if ((nextpage == -1) && (tuplesProcessed == maximumTuples)) {
      return false;
    }
    return true;
  }

  private int toInt(byte[] paramArrayOfByte, int paramInt){
    int i = 0;
    for (int j = 0; j < 4; j++) {
      i <<= 8;
      i |= paramArrayOfByte[(paramInt + j)] & 0xFF;
    }
    return i;
  }


  public byte[] next(){
    int i;
    if (tuplesProcessed == maximumTuples){
      if (nextpage == 0)
        return null;
      currentBuffer = new byte[pageSize];
      try{
        storageObject.ReadPage(nextpage, currentBuffer);
        currentPage = nextpage;
        nextpage = toInt(currentBuffer, 4);
        maximumTuples = toInt(currentBuffer, 0);
        tuplesProcessed = 0;
        byte[] arrayOfByte1 = new byte[tuplelength];
        for (i = 0; i < tuplelength; i++) {
          arrayOfByte1[i] = currentBuffer[(8 + tuplelength * tuplesProcessed + i)];
        }
        tuplesProcessed += 1;
        return arrayOfByte1;
      } catch (Exception localException) {
        localException.printStackTrace();
      }
    } else {
      byte[] arrayOfByte2 = new byte[tuplelength];
      for (i = 0; i < tuplelength; i++) {
        arrayOfByte2[i] = currentBuffer[(8 + tuplelength * tuplesProcessed + i)];
      }
      tuplesProcessed += 1;
      return arrayOfByte2;
    }
    return null;
  }


  public void open() throws Exception{
    currentBuffer = new byte[1024];
    storageObject.ReadPage(currentPage, currentBuffer);
    nextpage = toInt(currentBuffer, 4);
    maximumTuples = toInt(currentBuffer, 0);
    tuplesProcessed = 0;
  }

  public void close(){

  }
}
