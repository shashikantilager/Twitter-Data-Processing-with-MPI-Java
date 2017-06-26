
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import mpi.*;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author silager . StudentID: 856449
 *
 */

public class MPITwitterAnalysis{

	
	public static void main(String[] args) throws IOException,  ParseException {
		
		String mpiArgs[] = MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size(); 
		int master=0; int tag=100;
		int arrayLength =0;
		
		long startTime = System.currentTimeMillis();
		
		//Master
		if (rank ==0)
		{
			
			if (mpiArgs.length != 1){
			 	
			 	System.out.println("Improper COMMAND LINE ARGS:" + "\n" + "USAGE: InputJsonFileName " );
				System.exit(0);
			}

			String fileName =   mpiArgs[0];
		    int[] totalTweetCount = new int [16];
		
  			// If only 1 MPI process, Master (0) Works as both master and worker else assign workload to worker
 			if (size == 1)
 			{
 				ProcessMelbGrid processMelbGrid = new ProcessMelbGrid();
		   	    List <MelbGridModel> melbGrid ;

		   	 	melbGrid = processMelbGrid.parseJson();
 				melbGrid = readAndParseJsonTweet(fileName, melbGrid);

 				totalTweetCount = getResultInArray(melbGrid);

 			}
 			
 			// Assign Work to the Workers 
 			else 
 			{
 				
 				readAndParseJsonTweet(fileName); // 
		  		
	  			
				int[] recvTweetCountMaster = new int[16];
				
				//Collect The Result form Worker Process
				for (int i=1; i< size ; i++){
					
					MPI.COMM_WORLD.Recv(recvTweetCountMaster, 0, 16, MPI.INT, i, tag);
					
					
					for (int j=0; j< totalTweetCount.length ; j++){
						totalTweetCount[j] = totalTweetCount[j] + recvTweetCountMaster[j];
					}
				}

			}

			long endTime   = System.currentTimeMillis();
			long totalTime = endTime - startTime;
			
			OutputUtility ouputUtility = new OutputUtility();
			ouputUtility.processRequiredFormatOutputs(totalTweetCount, totalTime);
			
			
       }

      // workers - > Only when Total number of MPI process > 1
      else
      {
       		// If more than 1 MPI process, this Master Worker method works, else Master acts as Both Worker and Master
   			 if ( size > 1)
   			 {

	   			 ProcessMelbGrid processMelbGrid = new ProcessMelbGrid();
		   		 List <MelbGridModel> melbGrid ;
		   	 	 melbGrid = processMelbGrid.parseJson();

		   	 	 int [] revCompleteFlag = new int[1];
	   			 int [] sendTweetCountWorker = new int[16]; // 16- > total number of boxes in grid
	   			
	   			double[] tweetCoordinates = new double [2];
	   			int[] sendCompleteFlagFalse = new int [1];
	   			sendCompleteFlagFalse[0] = -1;

	   			while (sendCompleteFlagFalse[0] == -1) // master sends this value as 1 when file read is complete
	   			{
		   			MPI.COMM_WORLD.Recv(tweetCoordinates, 0, 2, MPI.DOUBLE, master, tag); 
		   			MPI.COMM_WORLD.Recv(sendCompleteFlagFalse, 0, 1, MPI.INT, master, tag);

		   		   	getTweetCountinMelbourne(tweetCoordinates, melbGrid);
	   			
	   			}

		    	 //Send the result to the Master process
		    	 int[] tweetCountResult = getResultInArray(melbGrid);
		    	 MPI.COMM_WORLD.Send(tweetCountResult, 0, tweetCountResult.length, MPI.INT, master, tag);
	    	}
		  	   
	 }
      		
        MPI.Finalize();
	
	}

	
	public static void readAndParseJsonTweet(String fileName) throws IOException,  ParseException{
		
		String filePath = "/home/silager/";
		filePath += fileName;
		FileInputStream inputStream = null;
		Scanner sc = null;
		JSONParser jsonParser = new JSONParser();
		int totalworkers = (MPI.COMM_WORLD.Size() -1);
		
		int tweetId=0;
		try
		{
		    inputStream = new FileInputStream(filePath);
		    sc = new Scanner(inputStream, "UTF-8");
		    
		    while (sc.hasNextLine())
		    {
		        String line = sc.nextLine();
		    
		        if( checkValidTweet(line) ){
			        
		            String jsonLine=  getJsonLine(line);
			        	
		            Object obj = jsonParser.parse(jsonLine);
		            JSONObject jsonObject = (JSONObject) obj;
			        JSONObject jsonTweet= (JSONObject) jsonObject.get("json");
			        JSONObject jsonCoordinate= (JSONObject) jsonTweet.get("coordinates");
			        
			        JSONArray jsonTweetCoordinates = (JSONArray) jsonCoordinate.get("coordinates");
			        
			        double []  tweetCoordinates = new double[2];
			        tweetCoordinates[0] = Double.parseDouble(jsonTweetCoordinates.get(0).toString());
			        tweetCoordinates[1] = Double.parseDouble(jsonTweetCoordinates.get(1).toString());
			       
			        int workerId = tweetId % totalworkers;
		            workerId = workerId+1; // + 1 because, 0 is master process,  This modulo logic circularly sends the tweet to each worker proceeses

		        	 int [] sendCompleteFlagFalse = new int[1];
		        	 sendCompleteFlagFalse[0] = -1;
		      
		        	MPI.COMM_WORLD.Send(tweetCoordinates, 0, 2, MPI.DOUBLE, workerId, 100); 
			       
			       	MPI.COMM_WORLD.Send(sendCompleteFlagFalse, 0, 1, MPI.INT, workerId, 100);  //indicates there are more line to be processed
					 tweetId++;  

		        }
		         

		    }// end of while

		    // This MPI messages indiactes File Read is complete, and terminate while loop in worker
		    int [] sendCompleteFlag = new int[1];
		    sendCompleteFlag[0] = 1; // indicates terminate while loop in worker
		    for (int i =1; i<MPI.COMM_WORLD.Size(); i++ )
		    {
		    	 double [] dummytweet  = new double[2];
		    	 MPI.COMM_WORLD.Send(dummytweet, 0, 2, MPI.DOUBLE,  i, 100); 
		    	 MPI.COMM_WORLD.Send(sendCompleteFlag, 0, 1, MPI.INT, i, 100);
		 	}
		    
		    if (sc.ioException() != null)
		    {
		        throw sc.ioException();
		    }
		    
		} 

		finally 
		{
		    if (inputStream != null)
		    {
		        inputStream.close();
		    }
		    
		    if (sc != null)
		    {
		        sc.close();
		    }
		}

		
	}


	// Override Method, only called when Master is acting as worker
	public static List <MelbGridModel>  readAndParseJsonTweet(String fileName, List<MelbGridModel> melbGrid) throws IOException,  ParseException
	{

		String filePath = "/home/silager/";
		filePath += fileName;
		FileInputStream inputStream = null;
		Scanner sc = null;
		JSONParser jsonParser = new JSONParser();
		int totalworkers = (MPI.COMM_WORLD.Size() -1);

		int tweetId=0;
		try
		{
		    inputStream = new FileInputStream(filePath);
		    sc = new Scanner(inputStream, "UTF-8");
		    
		    while (sc.hasNextLine())
		    {
		        String line = sc.nextLine();
		    
		        if( checkValidTweet(line) ){
			        
		            String jsonLine=  getJsonLine(line);
			        	
		            Object obj = jsonParser.parse(jsonLine);
		            JSONObject jsonObject = (JSONObject) obj;
			        JSONObject jsonTweet= (JSONObject) jsonObject.get("json");
			        JSONObject jsonCoordinate= (JSONObject) jsonTweet.get("coordinates");
			        
			        JSONArray jsonTweetCoordinates = (JSONArray) jsonCoordinate.get("coordinates");
			        
			        double []  tweetCoordinates = new double[2];
			        tweetCoordinates[0] = Double.parseDouble(jsonTweetCoordinates.get(0).toString());
			        tweetCoordinates[1] = Double.parseDouble(jsonTweetCoordinates.get(1).toString());
			       
			        getTweetCountinMelbourne(tweetCoordinates, melbGrid);
			        tweetId++;  

		        }
		         
		    }// end of while

		    if (sc.ioException() != null)
		    {
		        throw sc.ioException();
		    }
		    
		} 

		finally 
		{
		    if (inputStream != null)
		    {
		        inputStream.close();
		    }
		    
		    if (sc != null)
		    {
		        sc.close();
		    }
		}

		return melbGrid;
 
	}
	
	
	public static void getTweetCountinMelbourne(double [] tweetCoordinates, List <MelbGridModel> melbGrid  )  throws IOException,  ParseException{
	   
			 for (MelbGridModel currentBox: melbGrid)
			{
	         	
				 if (tweetCoordinates[0] >= currentBox.xMin && tweetCoordinates[1] >= currentBox.yMin &&
						 tweetCoordinates[0] <= currentBox.xMax &&  tweetCoordinates[1] <= currentBox.yMax )
				 {
					 
					 currentBox.incrementTweetCount();
					 break;
				 }
	     	}
     
	}


	public static double[] getdoubleArray( List<Double> coordianteList){

		double[] coordianteArray = new double[coordianteList.size()];
	 	
	 	for (int i = 0; i < coordianteList.size(); i++) {
	      
	    	coordianteArray[i] = (double)coordianteList.get(i);               
	 	}

	 	return coordianteArray;
	}
	

	public static String getJsonLine(String line){
		// remove the last  character , in each line to make it as valid json string
		 StringBuilder sb=new StringBuilder(line);
         int length = sb.length();
         if(sb.charAt(length -1) == ',')
        	 line=  (sb.deleteCharAt(sb.length()-1)).toString();
         
         return line;
         
	}
	
	public static boolean checkValidTweet(String line){

		 StringBuilder sb=new StringBuilder(line);
        int length = sb.length();
        if(sb.length() > 1 && sb.charAt (0) == '{' && sb.charAt(length -2) == '}')
        		return true;
        else 
        	return false;
		
	}
	
	
	public static int [] getResultInArray(List <MelbGridModel> melbGrid){

		int [] tweetCount = new int[16];
		
		for( MelbGridModel grid: melbGrid){
			int insertIndex= getInsertIndex(grid);
			tweetCount[insertIndex] = grid.tweetCount;
		}
		
		return tweetCount;
	}

	public static int getInsertIndex(MelbGridModel melbGrid){

		String boxId = melbGrid.Id;
		String[] idList = {"A1", "A2","A3", "A4", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4", "C5", "D3", "D4", "D5" };
        int idIndex = 0;
		for (int i=0; i< idList.length ; i++){
			if( boxId.equals(idList[i]) ){
				idIndex =i;
				break;
			}
		}

		return idIndex;
	}

	public static String getIdbyIndex(int index){
		
		String[] idList = {"A1", "A2","A3", "A4", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4", "C5", "D3", "D4", "D5" };

		return idList[index];
	}

}



class MelbGridModel {

	public  String Id;
	public double xMin;
	public double xMax;
	public double yMin;
	public double yMax;
	public int tweetCount;
	
	
	public MelbGridModel(){
		
	}
	
	public MelbGridModel(  String Id, double xMin, double xMax, double yMin, double yMax){
		this.Id = Id;
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;
		this.tweetCount = 0;
	}
	
	public int getTweetCount(){
		return this.tweetCount;
	}
	
	public void incrementTweetCount(){
		this.tweetCount += 1;
	}
	
}



 class ProcessMelbGrid {
	
	List <MelbGridModel> melbGrid ;
	
	 public  List <MelbGridModel> parseJson() throws FileNotFoundException, IOException, ParseException {

		JSONParser parser = new JSONParser();
		//String filePath = "C:\\Users\\silager\\Desktop\\Shashi\\Cloudsubject\\Assignemnt\\datafiles\\" ;
		String filePath= "/home/silager/";
	    filePath += "melbGrid.json";
	    String indent = "	";
	
        List <MelbGridModel>  melbGrid = new ArrayList<MelbGridModel>();
        try {
 
            Object obj = parser.parse(new FileReader(filePath));
            JSONObject jsonObject = (JSONObject) obj;
            JSONArray jsonFeautures = (JSONArray) jsonObject.get("features");
            
            for (int i =0; i<jsonFeautures.size() ; i++ ){
            	
            	JSONObject location =  (JSONObject) ((JSONObject)jsonFeautures.get(i)).get("properties");
       
            	String Id = location.get("id").toString();
            	double xMin = Double.parseDouble(location.get("xmin").toString());
            	double xMax = Double.parseDouble(location.get("xmax").toString());
            	double yMin = Double.parseDouble(location.get("ymin").toString());
            	double yMax = Double.parseDouble(location.get("ymax").toString());
            	
            	//System.out.println( Id + indent + xMin+ indent + xMin + indent+ xMax +indent+ yMin+ indent+ yMax);
            	MelbGridModel melbGridObj = new MelbGridModel( Id,  xMin,  xMax,  yMin,  yMax);
            	melbGrid.add(melbGridObj);
            	       	
            }
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
		return melbGrid;
	 }
}


class OutputUtility{

	public void  processRequiredFormatOutputs(int[] TweetCounts, long totalTime){

		Map<String, Integer> outPut = new HashMap<String, Integer>();

		for (int i=0;  i< TweetCounts.length; i++){

			String Id = getIdbyIndex(i);
			outPut.put(Id , TweetCounts[i]);
		}

		System.out.println("Time Taken To Execute: " + totalTime + " ms");
		System.out.println("##########UnSorted OutPut###########");
		printResult( outPut);
		Map<String, Integer> sortedOutput = sortByTweetCount(outPut);
		
		// Sorting the tweets
		System.out.println("###########Sorted OutPut###############");
		printResult(sortedOutput);

		Map<String, Integer> sortedOutputbyRow = getOrderbyRow(outPut);

		System.out.println("############Sorted Output by Row############");
		printResult(sortedOutputbyRow);

		Map<String, Integer> sortedOutputbyColumn = getOrderbyColumn(outPut);

		System.out.println("######## Sorted Output by Column'");
		printResult(sortedOutputbyColumn);
       
	}


	private Map<String, Integer> sortByTweetCount(Map<String, Integer> unsortMap)
    {
        List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(unsortMap.entrySet());
        
        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Integer>>()
        {
            public int compare(Entry<String, Integer> obj1,
                    Entry<String, Integer> obj2)
            {
                    return obj2.getValue().compareTo(obj1.getValue());

               
            }
        });

       
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Integer> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    private Map<String, Integer> getOrderbyRow(Map<String, Integer> outPut){

    	Map<String, Integer> orderbyRow = new HashMap<String, Integer>();

    	 // Getting Rowwise
		String [] rowA = { "A1" , "A2", "A3", "A4"}; 
		int rowATweets = getTweetCountsByKeyList(outPut,rowA);
		orderbyRow.put("A-Row" , rowATweets);

		//System.out.println("RowA- >" + rowATweets);
		String[] rowB = {"B1", "B2", "B3", "B4"};
		int rowBTweets = getTweetCountsByKeyList(outPut,rowB);
		orderbyRow.put("B-Row" , rowBTweets);

		String[] rowC = {"C1", "C2", "C3", "C4", "C5"};
		int rowCTweets = getTweetCountsByKeyList(outPut,rowC);
		orderbyRow.put("C-Row" , rowCTweets);

		String[] rowD = {"D3", "D4", "D5"};
		int rowDTweets = getTweetCountsByKeyList(outPut,rowD);
		orderbyRow.put("D-Row" , rowDTweets);

		Map<String, Integer> sortedOutputbyRow = sortByTweetCount(orderbyRow);

		return sortedOutputbyRow;

    }


    private Map<String, Integer> getOrderbyColumn(Map<String, Integer> outPut){

    	// getting columnwise
    	Map<String, Integer> orderbyCoulmn = new HashMap<String, Integer>();
		
		String[] column1 = {"A1", "B1", "C1"};
		int column1Tweets = getTweetCountsByKeyList(outPut,column1);
		orderbyCoulmn.put("Column 1" , column1Tweets);
		
		String[] column2 = {"A2", "B2", "C2"};
		int column2Tweets = getTweetCountsByKeyList(outPut,column2);
		orderbyCoulmn.put("Column 2" , column2Tweets);
		
		String[] column3 = {"A3", "B3", "C3", "D3"};
		int column3Tweets = getTweetCountsByKeyList(outPut,column3);
		orderbyCoulmn.put("Column 3" , column3Tweets);

		String[] column4 = {"A4", "B4", "C4", "D4"};
		int column4Tweets = getTweetCountsByKeyList(outPut,column4);
		orderbyCoulmn.put("Column 4" , column4Tweets);

		String[] column5 = {"C5" , "D5"};
		int column5Tweets = getTweetCountsByKeyList(outPut,column5);
		orderbyCoulmn.put("Column 5" , column5Tweets);

		Map<String, Integer> sortedOutputbyColumn = sortByTweetCount(orderbyCoulmn);

		return sortedOutputbyColumn;
    	
    }

    private int getTweetCountsByKeyList(Map<String, Integer> outPut, String[] Keys){

    	int totalTweets =0;
    	for (String key : Keys){

    		totalTweets += outPut.get(key);
    	}

    	return totalTweets;
    }


	private String getIdbyIndex(int index){
		
		String[] idList = {"A1", "A2","A3", "A4", "B1", "B2", "B3", "B4", "C1", "C2", "C3", "C4", "C5", "D3", "D4", "D5" };

		return idList[index];
	}


	public void printResult(Map<String, Integer> outPut){

		for (Map.Entry<String, Integer> entry : outPut.entrySet())
		 {
    			System.out.println( entry.getKey() + ": " + entry.getValue());
		 }
	}
	
}
	

