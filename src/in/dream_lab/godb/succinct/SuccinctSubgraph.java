package in.dream_lab.godb.succinct;
import java.util.ArrayList;
import java.util.List;


import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.buffers.SuccinctIndexedFileBuffer;
public class SuccinctSubgraph {
    private SuccinctIndexedFileBuffer succinctIndexedVertexFileBuffer,succinctIndexedEdgeFileBuffer;
    private SuccinctSubgraph(String vPath,String ePath)
    {
    	succinctIndexedVertexFileBuffer = new SuccinctIndexedFileBuffer(vPath, StorageMode.MEMORY_ONLY);
        succinctIndexedEdgeFileBuffer = new SuccinctIndexedFileBuffer(ePath, StorageMode.MEMORY_ONLY);
    }
    public static SuccinctSubgraph createSubgraph(String vPath, String ePath)
    {
    	return new SuccinctSubgraph(vPath, ePath);
    }
    public List<Long> getVertices(String name, String value,char delim)
    {
    	List<Long> vid = new ArrayList<>();
    	int offset;
    	String record;
    	String[] tokens;
    	Integer[] recordID = succinctIndexedVertexFileBuffer.recordSearchIds(value.getBytes());
    	for (Integer rid : recordID)
    	{
    		offset = succinctIndexedVertexFileBuffer.getRecordOffset(rid);
    		record=succinctIndexedVertexFileBuffer.extractUntil(offset, delim);
    		
    		tokens=record.split("\\W");
    		for(int i=0;i<tokens.length;i++) {
    			vid.add(Long.parseLong(tokens[i]));
    		}
    	}
    	return vid;
    }
    public List<Long> getOutEdges(String vid,char delim)
    {
        int offset;
        String[] tokens;
        String record;
        List<Long> sink = new ArrayList<>();
        Integer[] recordID=succinctIndexedEdgeFileBuffer.recordSearchIds(vid.getBytes());

        for (Integer rid : recordID)
        {
            offset = succinctIndexedEdgeFileBuffer.getRecordOffset(rid);
            record=succinctIndexedEdgeFileBuffer.extractUntil(offset, delim);

            tokens=record.split("\\W");
            // TODO: Implement Better Solution for below FOR loop @Swapnil
            for(int i=3;i<tokens.length;i++) {
                sink.add(Long.parseLong(tokens[i]));
            }
        }

            return sink;
    }
    public String getPropforVertex(String vid, int index)
    {
        int offset;
        String[] tokens;
        String record;
        Integer[] recordID=succinctIndexedVertexFileBuffer.recordSearchIds(vid.getBytes());
        offset = succinctIndexedVertexFileBuffer.getRecordOffset(recordID[0]);
        record=succinctIndexedVertexFileBuffer.extractUntil(offset, '|');
        tokens=record.split("\\W");
        return tokens[index+1];
    }
}
