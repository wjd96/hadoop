package hbaseNew;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class Myprocessor extends BaseRegionObserver {
	

	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,final Put put,final WALEdit edit,final Durability durability){
		List<Cell> cells=new ArrayList<Cell>();
		cells=put.get(Bytes.toBytes("base_info"),Bytes.toBytes("name"));
		int i=0;
		for(Cell cell:cells){
			String lower=new String(cell.getValue());
			String upper=lower.toUpperCase();
		//	cells.remove(i);
			put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("NAME"), Bytes.toBytes(upper));
		//	i++;
		}

	}

}
