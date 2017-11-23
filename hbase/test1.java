package hbaseNew;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.HColumnDescriptor; 
import org.apache.hadoop.hbase.HTableDescriptor; 
import org.apache.hadoop.hbase.KeyValue; 
import org.apache.hadoop.hbase.MasterNotRunningException; 
import org.apache.hadoop.hbase.ZooKeeperConnectionException; 
import org.apache.hadoop.hbase.client.Delete; 
import org.apache.hadoop.hbase.client.Get; 
import org.apache.hadoop.hbase.client.HBaseAdmin; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.HTablePool; 
import org.apache.hadoop.hbase.client.Put; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.filter.Filter; 
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService.exists_result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes; 
public class test1 {
	
	 public static Configuration configuration; 
	    static { 
	        configuration = HBaseConfiguration.create(); 
	        configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
	        configuration.set("hbase.zookeeper.quorum", "192.168.127.130"); 
	        configuration.set("hbase.master", "192.168.127.130:60010"); 
	    } 
	public static void insert() throws IOException{
	HTable table1=new HTable(configuration,"table1");
	    Put name=new Put(Bytes.toBytes("0003"));
	    name.add(Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes("m"));
	    Put age=new Put(Bytes.toBytes("0002"));
	    age.add(Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes("a"));	  
	 //   Put age=new Put(Bytes.toBytes("0001"));
//	    age.add(Bytes.toBytes("extra_info"), Bytes.toBytes("age"), Bytes.toBytes("a"));	
	    ArrayList<Put> puts=new ArrayList<>();
	    puts.add(name);
	    puts.add(age);
	    table1.put(puts);
	    table1.close();
	  /*  HTable t1=new HTable(configuration,"table1");
		int count=10;
		t1.setAutoFlush(false);
		while(count++<20){
		
		Put id =new Put(Bytes.toBytes(String.valueOf(count)));
		id.add(Bytes.toBytes("base_info"),Bytes.toBytes("id"),Bytes.toBytes(String.valueOf(count)));
		Put name =new Put(Bytes.toBytes(String.valueOf(count)));
		name.add(Bytes.toBytes("extra_info"),Bytes.toBytes("name"),Bytes.toBytes("wff"+String.valueOf(count)));
		Put gender =new Put(Bytes.toBytes(String.valueOf(count)));
		gender.add(Bytes.toBytes("extra_info"),Bytes.toBytes("gender"),Bytes.toBytes("w"+String.valueOf(count)));
		 ArrayList<Put> puts=new ArrayList<>();
		 puts.add(id);
		    puts.add(name);
		    puts.add(gender);
		t1.put(puts);
		}
		t1.flushCommits();
		t1.close();*/
	}
	public static void getdata() throws IOException{
		HTable table=new HTable(configuration,"table1");
	/*	int count=0;
		while(count++<10){
		Get get=new Get(Bytes.toBytes(count));
		Result result=table.get(get);
		List<Cell> cells=result.listCells();
		for(KeyValue kv:result.list()){
			String family=new String(kv.getFamily());
			System.out.println(family);
			String qualifier=new String(kv.getQualifier());
			System.out.println(qualifier);
			System.out.println(new String(kv.getValue()));
		}
		}*/
/*		int count=0;
		ArrayList<Get> gets=new ArrayList();
		while(count++<10){
			Get get=new Get(Bytes.toBytes(count));
		//	Result result=table.get(get);
			
			gets.add(get);
		}
		Result[] results=table.get(gets);
		for(Result result:results)
		for(KeyValue kv:result.list()){
			System.out.println(new String(kv.getFamily())+" "+new String(kv.getQualifier())+" "+new String(kv.getValue()));
		}
		while(count++<20){
			Get get1=new Get(Bytes.toBytes(String.valueOf(count)));
			Result re=table.get(get1);
			for(KeyValue kv:re.list()){
				System.out.println(new String(kv.getFamily())+" "+new String(kv.getQualifier())+" "+new String(kv.getValue()));		}
		}*/
		Get get=new Get(Bytes.toBytes("11"));
		Result result=table.get(get);
	/*	for(KeyValue kv:result.list()){
			String family=new String(kv.getFamily());
			System.out.println(family);
			String qualifier=new String(kv.getQualifier());
			System.out.println(qualifier);
			System.out.println(new String(kv.getValue()));
		}*/
	//	List<Cell> cells=new ArrayList<Cell>();
		//List<Cell> temp=new List<Cell>();
		//temp=result.listCells();
/*		Iterator<Cell> cells=result.listCells().iterator();
	//	System.out.println(cells.size());
		int i=0;
	while(cells.hasNext()){
		Cell cell=cells.next();
		String str=new String(cell.getFamily());
		if(str.equals("base_info"))
			cells.remove();
	}
//	List<Cell> cellss=new ArrayList<Cell>();
	Iterator<Cell> cellss=result.listCells().iterator();

	while(cellss.hasNext()){
		Cell cell=cells.next();
		System.out.println(new String(cell.getFamily())+new String(cell.getQualifier())+new String(cell.getValue()));

	}
*/
	}
	public static void deletedata() throws IOException{
		HTable table=new HTable(configuration,"table1");
		int count =0;
	/*	while(count++<10){
		Delete delete=new Delete(Bytes.toBytes(count));
		delete.deleteColumn(Bytes.toBytes("base_info"),Bytes.toBytes("id"));
	//	Delete name =new Delete(Bytes.toBytes(count));
		delete.deleteColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("name"));
		//Delete gender=new Delete(Bytes.toBytes(count));
		delete.deleteColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("gender"));
		table.delete(delete);
		}
		
		while(count++<20){
			Delete delete=new Delete(Bytes.toBytes(String.valueOf(count)));
			delete.deleteColumn(Bytes.toBytes("base_info"),Bytes.toBytes("id"));
		//	Delete name =new Delete(Bytes.toBytes(count));
			delete.deleteColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("name"));
			//Delete gender=new Delete(Bytes.toBytes(count));
			delete.deleteColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("gender"));
			table.delete(delete);
			table.close();
			}*/
		Delete delete=new Delete(Bytes.toBytes(String.valueOf("row1")));
		//delete.deleteColumn(Bytes.toBytes("base_info"),Bytes.toBytes("id"));
	//	Delete name =new Delete(Bytes.toBytes(count));
		delete.deleteColumn(Bytes.toBytes("base_info"),Bytes.toBytes("name"));
		//Delete gender=new Delete(Bytes.toBytes(count));
	//	delete.deleteColumn(Bytes.toBytes("base_info"),Bytes.toBytes("gender"));
		table.delete(delete);
		table.close();
	}
	public static void batch() throws IOException{
		HTable table=new HTable(configuration,"table1");
		List<Row> batch=new ArrayList<Row>();
		Put put=new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("base_info"),Bytes.toBytes("name"),Bytes.toBytes("wff"));
		batch.add(put);
		Get get=new Get(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
		batch.add(get);
		Delete delete=new Delete(Bytes.toBytes("0002"));
		delete.deleteColumn(Bytes.toBytes("base_info"), Bytes.toBytes("age"));
		batch.add(delete);
		Object[] results=new Object[batch.size()];
		try{
			table.batch(batch,results);
		}catch(Exception e){
			System.out.println("worry");
		}
		for(int i=0;i<results.length;i++){
			if(results.getClass().toString().equals(Result.class)){
			Result res=(Result)results[i];
	//		if()
			for(KeyValue kv:res.list()){
				System.out.println(new String(kv.getFamily())+new String(kv.getQualifier())+new String(kv.getValue()));
			}
			}
		}
	}
	public static void scan() throws IOException{
		HTable table=new HTable(configuration,"table1");
		Scan scan=new Scan();
		//scan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("id"));
		scan.addFamily(Bytes.toBytes("extra_info"));
		scan.setStartRow(Bytes.toBytes("11"));
		scan.setStopRow(Bytes.toBytes("15"));
		ResultScanner scanner=table.getScanner(scan);
		System.out.println("1");
		for(Result res:scanner){
		//	System.out.println("1");
			for(KeyValue kv:res.list()){
				System.out.println(new String(kv.getFamily())+new String(kv.getQualifier())+new String(kv.getValue()));
				
			}
		}
		scanner.close();
		table.close();
		
	}
	public static void filter() throws IOException{
		HTable table=new HTable(configuration,"table1");
		Scan scan=new Scan();
	//	Filter filter1=new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("15")));
	//	Filter filter1=new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("extra_info")));
	//	Filter filter1=new QualifierFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("name")));
	//	Filter filter3=new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("w"));

		Filter filter1=new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("w"));
	//	SingleColumnValueFilter filter1=new SingleColumnValueFilter(Bytes.toBytes("extra_info"),Bytes.toBytes("name"),CompareFilter.CompareOp.NOT_EQUAL,new SubstringComparator("15"));
	//	filter1.setFilterIfMissing(true);
	//	Filter filter1=new PrefixFilter(Bytes.toBytes("wff"));
		Filter filter2=new SkipFilter(filter1);
		List<Filter> filters=new ArrayList<Filter>();
		filters.add(filter2);
		FilterList filterList1=new FilterList(filters);
		scan.setFilter(filterList1);
		ResultScanner scanner=table.getScanner(scan);
		for(Result res:scanner){
			for(KeyValue kv:res.list()){
				System.out.println(new String(kv.getFamily())+new String(kv.getQualifier())+new String(kv.getValue()));
				
			}
		}
		scanner.close();
		table.close();
		
	}

/*	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// TODO Auto-generated method stub

		Configuration conf=HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");  
        conf.set("hbase.zookeeper.quorum", "192.168.127.130");  
        conf.set("hbase.master", "192.168.127.130:60010"); 
        System.out.println("1111111111");
		HBaseAdmin admin=new HBaseAdmin(configuration);
		System.out.println("1111111111");
		
	//	TableName name=TableName.valueOf("table1");
	//	HTableDescriptor desc=new HTableDescriptor(name);
	//	HColumnDescriptor base_info=new HColumnDescriptor("base_info");
	//	HColumnDescriptor extra_info=new HColumnDescriptor("extra_info");
	//	base_info.a
	//	base_info.setMaxVersions(5);
	//	desc.addFamily(base_info);
	//	desc.addFamily(extra_info);
	//	admin.createTable(desc);
		HTableDescriptor des=admin.getTableDescriptor(Bytes.toBytes("table1"));
		HColumnDescriptor addition_info=new HColumnDescriptor("addition_info");
		des.addFamily(addition_info);
		admin.disableTable(Bytes.toBytes("table1"));
		admin.modifyTable(Bytes.toBytes("table1"),des);
		admin.enableTable(Bytes.toBytes("table1"));
	//	insert();
	//	getdata();
	//	deletedata();
	//	batch();
	//	scan();
	//	filter();
		String str1="abcd";
		System.out.println(str1.toUpperCase());
	//	HTable t1=new HTable(configuration,"table1");
		//System.out.println(t1.getRegionLocation(Bytes.toBytes("11")));
		System.out.println("over");
		
		
	}
	/*  public static Configuration configuration; 
	    static { 
	        configuration = HBaseConfiguration.create(); 
	        configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
	        configuration.set("hbase.zookeeper.quorum", "192.168.127.130"); 
	        configuration.set("hbase.master", "192.168.127.130:60010"); 
	    } 
	 
	/*    public static void main(String[] args) throws IOException { 
	         createTable("wujintao"); 
	         System.out.println("111");
	         insertData("wujintao"); 
	         System.out.println("1111");
	        // QueryAll("wujintao"); 
	        // QueryByCondition1("wujintao"); 
	        // QueryByCondition2("wujintao"); 
	        //QueryByCondition3("wujintao"); 
	        //deleteRow("wujintao","abcdef"); 
	        //deleteByCondition("wujintao","abcdef"); 
	    } 
	 
	     
	    public static void createTable(String tableName) { 
	        System.out.println("start create table ......"); 
	        try { 
	            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
	            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建 
	                hBaseAdmin.disableTable(tableName); 
	                hBaseAdmin.deleteTable(tableName); 
	                System.out.println(tableName + " is exist,detele...."); 
	            } 
	            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName); 
	            tableDescriptor.addFamily(new HColumnDescriptor("column1")); 
	            tableDescriptor.addFamily(new HColumnDescriptor("column2")); 
	            tableDescriptor.addFamily(new HColumnDescriptor("column3")); 
	            hBaseAdmin.createTable(tableDescriptor); 
	        } catch (MasterNotRunningException e) { 
	            e.printStackTrace(); 
	        } catch (ZooKeeperConnectionException e) { 
	            e.printStackTrace(); 
	        } catch (IOException e) { 
	            e.printStackTrace(); 
	        } 
	        System.out.println("end create table ......"); 
	    } 
	 
	     
	    public static void insertData(String tableName) throws IOException { 
	        System.out.println("start insert data ......"); 
	     //   HTablePool pool = new HTablePool(configuration, 1000); 
	        HTable table = new HTable(configuration,tableName); 
	        Put put = new Put("112233bbbcccc".getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值 
	        put.add("column1".getBytes(), null, "aaa".getBytes());// 本行数据的第一列 
	        put.add("column2".getBytes(), null, "bbb".getBytes());// 本行数据的第三列 
	        put.add("column3".getBytes(), null, "ccc".getBytes());// 本行数据的第三列 
	        try { 
	            table.put(put); 
	        } catch (IOException e) { 
	            e.printStackTrace(); 
	        } 
	        System.out.println("end insert data ......"); 
	    } 
	 
	     
	    public static void dropTable(String tableName) { 
	        try { 
	            HBaseAdmin admin = new HBaseAdmin(configuration); 
	            admin.disableTable(tableName); 
	            admin.deleteTable(tableName); 
	        } catch (MasterNotRunningException e) { 
	            e.printStackTrace(); 
	        } catch (ZooKeeperConnectionException e) { 
	            e.printStackTrace(); 
	        } catch (IOException e) { 
	            e.printStackTrace(); 
	        } 
	 
	    } 
	     
	     public static void deleteRow(String tablename, String rowkey)  { 
	        try { 
	            HTable table = new HTable(configuration, tablename); 
	            List list = new ArrayList(); 
	            Delete d1 = new Delete(rowkey.getBytes()); 
	            list.add(d1); 
	             
	            table.delete(list); 
	            System.out.println("删除行成功!"); 
	             
	        } catch (IOException e) { 
	            e.printStackTrace(); 
	        } 
	         
	 
	    } 
	 
	      
	     public static void deleteByCondition(String tablename, String rowkey)  { 
	            //目前还没有发现有效的API能够实现根据非rowkey的条件删除这个功能能，还有清空表全部数据的API操作 
	 
	    } 
	 
	 
	     
	    public static void QueryAll(String tableName) { 
	        HTablePool pool = new HTablePool(configuration, 1000); 
	        HTable table = (HTable) pool.getTable(tableName); 
	        try { 
	            ResultScanner rs = table.getScanner(new Scan()); 
	            for (Result r : rs) { 
	                System.out.println("获得到rowkey:" + new String(r.getRow())); 
	                for (KeyValue keyValue : r.raw()) { 
	                    System.out.println("列：" + new String(keyValue.getFamily()) 
	                            + "====值:" + new String(keyValue.getValue())); 
	                } 
	            } 
	        } catch (IOException e) { 
	            e.printStackTrace(); 
	        } 
	    } 
	 
	     
	    public static void QueryByCondition1(String tableName) { 
	 
	        HTablePool pool = new HTablePool(configuration, 1000); 
	        HTable table = (HTable) pool.getTable(tableName); 
	        try { 
	            Get scan = new Get("abcdef".getBytes());// 根据rowkey查询 
	            Result r = table.get(scan); 
	            System.out.println("获得到rowkey:" + new String(r.getRow())); 
	            for (KeyValue keyValue : r.raw()) { 
	                System.out.println("列：" + new String(keyValue.getFamily()) 
	                        + "====值:" + new String(keyValue.getValue())); 
	            } 
	        } catch (IOException e) { 
	            e.printStackTrace(); 
	        } 
	    } 
	 
	     
	    public static void QueryByCondition2(String tableName) { 
	 
	        try { 
	            HTablePool pool = new HTablePool(configuration, 1000); 
	            HTable table = (HTable) pool.getTable(tableName); 
	            Filter filter = new SingleColumnValueFilter(Bytes 
	                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes 
	                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询 
	            Scan s = new Scan(); 
	            s.setFilter(filter); 
	            ResultScanner rs = table.getScanner(s); 
	            for (Result r : rs) { 
	                System.out.println("获得到rowkey:" + new String(r.getRow())); 
	                for (KeyValue keyValue : r.raw()) { 
	                    System.out.println("列：" + new String(keyValue.getFamily()) 
	                            + "====值:" + new String(keyValue.getValue())); 
	                } 
	            } 
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        } 
	 
	    } 
	 
	     
	    public static void QueryByCondition3(String tableName) { 
	 
	        try { 
	            HTablePool pool = new HTablePool(configuration, 1000); 
	            HTable table = (HTable) pool.getTable(tableName); 
	 
	            List<Filter> filters = new ArrayList<Filter>(); 
	 
	            Filter filter1 = new SingleColumnValueFilter(Bytes 
	                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes 
	                    .toBytes("aaa")); 
	            filters.add(filter1); 
	 
	            Filter filter2 = new SingleColumnValueFilter(Bytes 
	                    .toBytes("column2"), null, CompareOp.EQUAL, Bytes 
	                    .toBytes("bbb")); 
	            filters.add(filter2); 
	 
	            Filter filter3 = new SingleColumnValueFilter(Bytes 
	                    .toBytes("column3"), null, CompareOp.EQUAL, Bytes 
	                    .toBytes("ccc")); 
	            filters.add(filter3); 
	 
	            FilterList filterList1 = new FilterList(filters); 
	 
	            Scan scan = new Scan(); 
	            scan.setFilter(filterList1); 
	            ResultScanner rs = table.getScanner(scan); 
	            for (Result r : rs) { 
	                System.out.println("获得到rowkey:" + new String(r.getRow())); 
	                for (KeyValue keyValue : r.raw()) { 
	                    System.out.println("列：" + new String(keyValue.getFamily()) 
	                            + "====值:" + new String(keyValue.getValue())); 
	                } 
	            } 
	            rs.close(); 
	 
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        } 
	 
	    } 
	 
*/
}
