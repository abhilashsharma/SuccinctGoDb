package in.dream_lab.godb.succinct;
/**
 * Created by sandy on 8/25/17.
 */

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.io.*;
public class Executor {
   
    
    

    //Succinct DataStructures
	static HashMap<String,Integer> propToIndex= new HashMap<String,Integer>();
    //GODB functions
	/**
	 * Initialize the class variables
	 * This method is called in first superstep, it parses the query passed.
	 * It also reads the Graph statistics(Called as Heuristics) from disk
	 */
	private static void init(String Arguments){
		String arguments = Arguments;
	  
	
		
	    path = new ArrayList<Step>();
		Executor.Step.Type previousStepType = Executor.Step.Type.EDGE;
		for(String _string : arguments.split(Pattern.quote("//"))[0].split(Pattern.quote("@")) ){
			if(_string.contains("?")){
				if(previousStepType == Executor.Step.Type.EDGE)
					path.add(new Step(Executor.Step.Type.VERTEX,null, null, null));
				previousStepType = Executor.Step.Type.EDGE;
				String[] _contents = _string.split(Pattern.quote("?")); 
				String p = null ;
				Object v = null ;
				Executor.Step.Direction d = (_contents[0].equals("out") ) ? Executor.Step.Direction.OUT : Executor.Step.Direction.IN;
				if ( _contents.length > 1 )	{
					p = _contents[1].split(Pattern.quote(":"))[0];
					String typeAndValue = _contents[1].split(Pattern.quote(":"))[1];
					String type = typeAndValue.substring(0, typeAndValue.indexOf("["));
					if(type.equals("float")) {
						v = Float.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );
					}
					else if(type.equals("double")) { 
						v = Double.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

					}
					else if(type.equals("int")) { 
						v = Integer.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

					}
					else { 
						v = String.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

					}
				}
				path.add(new Step(Executor.Step.Type.EDGE, d, p, v));
			}
			else{
				previousStepType = Executor.Step.Type.VERTEX;
				String p = _string.split(Pattern.quote(":"))[0];
				String typeAndValue = _string.split(Pattern.quote(":"))[1];
				Object v = null;
				String type = typeAndValue.substring(0, typeAndValue.indexOf("["));
				if(type.equals("float")) {
					v = Float.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );
				}
				else if(type.equals("double")) { 
					v = Double.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

				}
				else if(type.equals("int")) { 
					v = Integer.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

				}
				else { 
					v = String.valueOf(typeAndValue.substring(typeAndValue.indexOf("[") + 1, typeAndValue.indexOf("]")) );

				}

				path.add(new Step(Executor.Step.Type.VERTEX,null, p, v));
			}
			

		}
		if(previousStepType == Executor.Step.Type.EDGE){
			path.add(new Step(Executor.Step.Type.VERTEX,null, null, null));
		}
		noOfSteps = path.size();
//		queryCostHolder = new Double[getSubgraph().getSubgraphValue().noOfSteps];
//		for (int i = 0; i < getSubgraph().getSubgraphValue().queryCostHolder.length; i++) {
//			getSubgraph().getSubgraphValue().queryCostHolder[i] = new Double(0);
//			
//		}
		forwardLocalVertexList = new LinkedList<VertexMessageSteps>();
//		revLocalVertexList = new LinkedList<VertexMessageSteps>();
//		inVerticesMap = new HashMap<Long, HashMap<String,LinkedList<Long>>>();
//		remoteSubgraphMap = new HashMap<Long, Long>();
//		hueristics=HueristicsLoad.getInstance();//loading this at a different place

		
	}
	
	
	/**
	 * Utility function to compare two values
	 * 
	 */
	private static boolean compareValuesUtil(Object o,Object currentValue){
		if( o.getClass().getName() != currentValue.getClass().getName()){return false;}
		if (o instanceof Float){
			return ((Float)o).equals(currentValue);
		}
		else if (o instanceof Double){
			return ((Double)o).equals(currentValue);
		}
		else if (o instanceof Integer){
			return ((Integer)o).equals(currentValue);
		}
		else{
			return ((String)o).equals(currentValue);
		}
		
	}
	

    
	static LinkedList<VertexMessageSteps> forwardLocalVertexList;
	static ArrayList<Step> path = null;
	static long noOfSteps;
	static HashMap<Long,ResultSet> resultsMap = new HashMap<Long,ResultSet>();
    //GODB data structures
    /**
	 * Representative class to keep tab of next vertex to be processed, different for path query
	 */
	public static class VertexMessageSteps{
		Long queryId;
		Long vertexId;
		String message;
		Integer stepsTraversed;
		Long previousSubgraphId;
		Integer previousPartitionId;
		Long startVertexId;
		Integer startStep;
		VertexMessageSteps(Long _queryId,Long _vertexId,String _message,Integer _stepsTraversed,Long _startVertexId,Integer _startStep,Long _previousSubgraphId, Integer _previousPartitionId){
			this.queryId=_queryId;
			this.vertexId = _vertexId;
			this.message = _message;
			this.stepsTraversed = _stepsTraversed;
			this.previousSubgraphId = _previousSubgraphId;
			this.startVertexId = _startVertexId;
			this.startStep=_startStep;
			this.previousPartitionId = _previousPartitionId;
		}
	}
	
	
	public static class ResultSet{
		public ArrayList<String> forwardResultSet;
		public ArrayList<String> revResultSet;
		public ResultSet() {
			forwardResultSet = new ArrayList<String>();
			revResultSet = new ArrayList<String>();
		}
	}
	
	
	
	/**
	 * Class for storing the traversal path V->E->V->E->E.....
	 */
	public static class Step{
		public String property;
		public Object value;
		public Direction direction;
		public Type type;
		public static enum Type{EDGE,VERTEX}
		public static enum Direction{OUT,IN}
		
		//used in reachability query
		public Step(String p,Object v){
			this.property = p;
			this.value = v;
		}
		
		//used in path query
		public Step(Type t,Direction d,String p,Object v){
			this.type = t;
			this.direction = d;
			this.property = p;
			this.value = v;
		}

	}
    
	
	public static void compute(SuccinctSubgraph sg) {
//		System.out.println("FORLISTSIZE:"+ forwardLocalVertexList.size());
        while(!forwardLocalVertexList.isEmpty()) {
			VertexMessageSteps vertexMessageStep = forwardLocalVertexList.poll();
			//output(partition.getId(), subgraph.getId(), "FORWARD-LIST");
			/* if last step,end that iteration*/
			//System.out.println("Reached:" + vertexMessageStep.startVertexId + " Path Size:" + vertexMessageStep.stepsTraversed + "/" + (path.size()-1));
			if ( vertexMessageStep.stepsTraversed == path.size()-1 ){
					if ( !resultsMap.containsKey(vertexMessageStep.startVertexId) )
						resultsMap.put(vertexMessageStep.startVertexId, new ResultSet());
					//System.out.println("MESSAGE ADDED TO FORWARDRESULTSET:" + vertexMessageStep.message);
					resultsMap.get(vertexMessageStep.startVertexId).forwardResultSet.add(vertexMessageStep.message);
					
				continue;
			}
			
			Step nextStep = path.get(vertexMessageStep.stepsTraversed+1);
			
			
//			IVertex<MapValue, MapValue, LongWritable, LongWritable> currentVertex = getSubgraph().getVertexById(new LongWritable(vertexMessageStep.vertexId));
			
			if( nextStep.type == Executor.Step.Type.EDGE ) {
				
				if ( nextStep.direction == Executor.Step.Direction.OUT ) {
					/* null predicate handling*/
					//int count=0;
					boolean flag=false;
					boolean addFlag=false;
					if ( nextStep.property == null && nextStep.value == null ) {
//						System.out.println("OUTEDGESIZE:"+ sg.getOutEdges(vertexMessageStep.vertexId.toString()).size());
						for( Long sink: sg.getOutEdges(vertexMessageStep.vertexId.toString()+"@",'|')) {
							//count++;
//							System.out.println("Traversing edges");
							long otherVertex = sink;
							StringBuilder _modifiedMessage = new StringBuilder("");
							_modifiedMessage.append(vertexMessageStep.message).append("-->E:").append("-->V:").append(String.valueOf(otherVertex));
//							System.out.println("PATH:" + _modifiedMessage);
								forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertex,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
							
								
						}
					}
					
				}
//				else if ( nextStep.direction == Executor.Step.Direction.IN ) {
//
//					/* null predicate handling*/
//					boolean flag=false;
//					boolean addFlag=false;
//					if ( nextStep.property == null && nextStep.value == null ) {
//						
//						for(Tuple2<Long,Long> edge: sg.getInEdges(vertexMessageStep.vertexId.toString()) ) {
//							long otherVertexId = edge._1;
//							StringBuilder _modifiedMessage = new StringBuilder("");
//							_modifiedMessage.append(vertexMessageStep.message).append("<--E:").append(edge._2).append("<--V:").append(otherVertexId);
//								/* TODO :add the correct value to list*/
//								forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,otherVertexId,_modifiedMessage.toString(),vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
//							
//						
//						}
//					}
//					
//				}
				
			}
			else if ( nextStep.type == Executor.Step.Type.VERTEX ) {
				
				/* null predicate*/
				if( nextStep.property == null && nextStep.value == null ) {
					/* add appropriate value later*/
					forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
					//forwardLocalVertexList.add(vertexMessageStep);
				}
				/* filtered vertex*/
				else {
					
					
//					System.out.println("Property:" + nextStep.property + ", Value Returned:" + );
					if ( compareValuesUtil(String.valueOf(sg.getPropforVertex(vertexMessageStep.vertexId.toString() + "@",propToIndex.get(nextStep.property)  )), nextStep.value.toString()) ) {
						/* add appropriate value later*/
						forwardLocalVertexList.add(new VertexMessageSteps(vertexMessageStep.queryId,vertexMessageStep.vertexId,vertexMessageStep.message,vertexMessageStep.stepsTraversed+1, vertexMessageStep.startVertexId,vertexMessageStep.startStep, vertexMessageStep.previousSubgraphId, vertexMessageStep.previousPartitionId));
						//forwardLocalVertexList.add(vertexMessageStep);
					}
				}
				
			}
			
		}
	}
	
	public static void writeOutput() {
		
		for(Map.Entry<Long, ResultSet> entry: resultsMap.entrySet()) {
			if (!entry.getValue().revResultSet.isEmpty())
				for(String partialRevPath: entry.getValue().revResultSet) {
					if (!entry.getValue().forwardResultSet.isEmpty())
						for(String partialForwardPath: entry.getValue().forwardResultSet) {
							System.out.println("ResultSetBothNotEmpty:" +partialRevPath+partialForwardPath);
							//output(partition.getId(), subgraph.getId(), partialRevPath+partialForwardPath); 
						}
					else{
						System.out.println("ResultSetForwardEmpty:" +partialRevPath);
						//output(partition.getId(), subgraph.getId(), partialRevPath);
					}
				}
			else
				for(String partialForwardPath: entry.getValue().forwardResultSet) {
					System.out.println("ResultSetReverseEmpty:" +partialForwardPath);
					//output(partition.getId(), subgraph.getId(), partialForwardPath); 
				}
		}

	}
	
	public static void	warmup(String warmUpFile,SuccinctSubgraph sg) throws IOException {
		FileReader fr = new FileReader(warmUpFile);
		 String sCurrentLine;
        BufferedReader br = new BufferedReader(fr);
		while ((sCurrentLine = br.readLine()) != null) {
        	init(sCurrentLine);
            String currentProperty = path.get(0).property; 
    		Object currentValue = path.get(0).value;
    		Long start = System.currentTimeMillis();
    		makequery(currentProperty,currentValue,sg);
            compute(sg);
          Long time = System.currentTimeMillis()-start;
//          System.out.println("TIME:"+time);
          //commented for running experiments
//            writeOutput();
            clear();
            
        }
	}
	
	
    public static void main(String args[])throws ClassNotFoundException, IOException
    {
     
    	
    	propToIndex.put("patid", 0);
    	propToIndex.put("country", 1);
    	propToIndex.put("nclass", 2);
    	
    	//initializing and warming up
        SuccinctSubgraph sg = SuccinctSubgraph.createSubgraph(args[0], args[1]);
        warmup(args[3],sg);
        
        
        FileReader fr = new FileReader(args[2]);
        BufferedReader br = new BufferedReader(fr);

        String sCurrentLine;

        
        
        while ((sCurrentLine = br.readLine()) != null) {
        	init(sCurrentLine);
            String currentProperty = path.get(0).property; 
    		Object currentValue = path.get(0).value;
    		Long start = System.currentTimeMillis();
    		makequery(currentProperty,currentValue,sg);
            compute(sg);
          Long time = System.currentTimeMillis()-start;
          System.out.println("TIME:"+time);
          //commented for running experiments
//            writeOutput();
            clear();
            
        }
//        System.out.println("Scan Count:"+sg.count);
        br.close();
//        System.out.println("Second Run");
//        FileReader fr1 = new FileReader(args[2]);
//        BufferedReader br1 = new BufferedReader(fr1);
//        while ((sCurrentLine = br1.readLine()) != null) {
//        	init(sCurrentLine);
//            String currentProperty = path.get(0).property; 
//    		Object currentValue = path.get(0).value;
//    		Long start = System.currentTimeMillis();
//    		makequery(currentProperty,currentValue,sg);
//            compute(sg);
//          Long time = System.currentTimeMillis()-start;
//          System.out.println(time);
//            writeOutput();
//            clear();
//            
//        }
//        
//        br1.close();
        
        //"VID:string[5148523]@out?@VID:string[4665495]@out?@VID:string[4075620]//0//163"
        
        


//        File file = new File(args[2]);
//        BufferedReader warm = new BufferedReader(new FileReader(file));
//        String l;
//        while ((l = warm.readLine())!=null)
//          sg.getVertices(new AbstractMap.SimpleEntry<>("Country", l), true);
//        BufferedReader in = new BufferedReader(new FileReader(file));
//        String line;
//        Long start = System.currentTimeMillis();
//        while ((line = in.readLine())!=null)
//          sg.getVertices(new AbstractMap.SimpleEntry<>("Country", line), true);
//        Long time = System.currentTimeMillis()-start;
//        System.out.println(time);
    }
	private static void clear() {
		
		resultsMap.clear();
		path.clear();
		forwardLocalVertexList.clear();
		noOfSteps=0;
//		startPos=0;
		
	}
	private static void makequery(String currentProperty, Object currentValue,SuccinctSubgraph sg) {
	
		String value= (String)currentValue;
		List<Long> vList=sg.getVertices(currentProperty, (String)currentValue,'@');
		int count=0;
		for (long vid:vList){
			
			if(count%2==1) {	
				String _message = "V:"+String.valueOf(vid);
//				System.out.println("STARTING VERTEX:" + _message);
//				if (startPos == 0)
				forwardLocalVertexList.add( new VertexMessageSteps(1l,vid,_message, 0, vid,0,  0l, 0) );
				
			}
			count++;	
					
			}
		}
		
		
		
	
}
