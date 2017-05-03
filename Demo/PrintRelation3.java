package Demo;

public class PrintRelation3{
  public static void main(String args[]) throws Exception{
    System.out.println("The tuples of Employees with salary more than or equal to $50000 are: ");
    GetTupleFromRelationIterator2 relation3Iterator = new GetTupleFromRelationIterator2("myDisk1", 35, 0);
    String name = "";
    int salary = 0;
    relation3Iterator.open();
    while(relation3Iterator.hasNext()){
      byte [] tuple = relation3Iterator.next();
      name = new String(tuple).substring(4, 27);
      salary = toInt(tuple, 31);
      if( salary >= 50000){
        System.out.println( name + ", " + salary);
      }
    }

  }

  private static int toInt(byte[] bytes, int offset) {
		  int ret = 0;
		  for (int i=0; i<4; i++) {
		    ret <<= 8;
		    ret |= (int)bytes[offset+i] & 0xFF;
		  }
		  return ret;
		}

}
