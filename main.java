import java.math.BigInteger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.*;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import io.netty.util.internal.ThreadLocalRandom;
import scala.Tuple2;



class staticHashingOpen<K, V>  implements Serializable {
    class HashingStructure<K, V> implements Serializable {
        K key;
        V value;
        String state;

        public HashingStructure(String __state) {
            state = __state;
        }

        public HashingStructure(String __state, K __key, V __value) {
            key = __key;
            value = __value;
            state = __state;
        }
    }

    private int max_size;
    private int current_size = 0;
    private int remaining_spots;
    private ArrayList<HashingStructure<K, V>> myHashingTree;
    private String empty = "EMPTY";
    private String removed = "REMOVED";
    private String actif = "ACTIF";
    private static String csvsplitby = ",";

    public staticHashingOpen(int __max_size) {
        max_size = __max_size;
        current_size = 0;
        remaining_spots = __max_size;
        myHashingTree = new ArrayList<>();
        myHashingTree.ensureCapacity(__max_size);
        for (int i = 0; i < max_size; i++) {
            myHashingTree.add(new HashingStructure<>(empty));
        }
    }

    private int hash_code(K key) {
        return Math.abs(key.hashCode());
    }

    private int hashing_function(K key) {
        return hash_code(key) % max_size;
    }

    public void add(K key, V value) {
        if (remaining_spots == 0) {
            System.err.println("Hashing index is full!");
            return;
        }

        int hashing_code = hashing_function(key);
        int shift = 0;
        int spot;
        while (shift < max_size) {
            spot = (hashing_code + shift) % max_size;
            if (myHashingTree.get(spot).state.equals(empty)) {
                myHashingTree.set(spot, new HashingStructure<K,V>(actif, key, value));
                break;
            }
            shift++;
        }
        remaining_spots--;
        current_size++;
    }

    public void add_all(ArrayList<K> keys, ArrayList<V> values) {
        int size = keys.size(); 
        if (size != values.size()) {
            System.err.println("Keys' size and Values' size do not match!");
            return;
        }

        if (remaining_spots < size) {
            System.err.println("Remaining spots are less then arrays' size!");
            return;
        }

        for (int i = 0; i < size; i++) {
            add(keys.get(i), values.get(i));
        }
    }


    public V search(K key) {
        int hashing_code = hashing_function(key);
        int shift = 0;
        int spot;
        HashingStructure<K, V> element_to_check;
        while (shift < max_size) {
            spot = (hashing_code + shift) % max_size;
            element_to_check = myHashingTree.get(spot);
            if ((element_to_check.state.equals(actif)) && element_to_check.key.equals(key)) {
                return element_to_check.value;
            }
            shift++;
        }
        return null;
    }

    public void remove(K key) {
        int hashing_code = hashing_function(key);
        int shift = 0;
        int spot;
        HashingStructure<K, V> element_to_check;
        while (shift < max_size) {
            spot = (hashing_code + shift) % max_size;
            element_to_check = myHashingTree.get(spot);
            if ((element_to_check.state.equals(actif)) && element_to_check.key.equals(key)) {
                myHashingTree.set(spot, new HashingStructure<K, V>(removed));
                current_size--;
                return;
            }
            shift++;
        }
    }

    int get_current_size() {
        return current_size;
    }
}



    public class main {
    	public static void main(String[] args) throws IOException {
    	    //this example is from anadiotis slides..
    	    //when we have the dataset we can split it to as many threads we want
    	    //we have to test the code a bit more
    	    //overleaf (?)
    		String csvsplitby = ",";
    		int max_size = 670000;
    		
    	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkFileSumApp");
    	    JavaSparkContext sc = new JavaSparkContext(conf);
    	    staticHashingOpen<String,String> StaticHash = new staticHashingOpen<String,String>(max_size);
    	    JavaRDD<String> textFile = sc.textFile("911.csv");
    	    
    	    
    	    //Assign DataSet to Java Pair RDD with Keys and values
            JavaPairRDD<String,String> lines = textFile.mapToPair(x -> new Tuple2(x.split(",")[2], x.split(",")[4]));
            
            //here we measure the time that 50000 data need to insert and then we measure the access time.
            int min_index = 0;
    		int max_index = 0;
    		int nb_fold = 10;
    		int step_index = 50000;
    		int nb_search = 1000;
    		ArrayList<Long> insert_bloc_times = new ArrayList<>();
    		ArrayList<Long> search_bloc_times = new ArrayList<>();
    		List<Tuple2<String, String>> keys = lines.take(max_size);
    		for (int i = 0; i < nb_fold; i++) {
    			
    			// Extract step_index row 
    			max_index = min_index + step_index;
    			List<Tuple2<String, String>> temp = lines.take(max_index);
    			temp = temp.subList(min_index, max_index);
    			JavaRDD<Tuple2<String, String>> fold_lines = sc.parallelize(temp);
    			
    			//Insertion in has table using spark
    			 long start = System.nanoTime();
    			 fold_lines.map(new Function<Tuple2<String, String>, String>() {
    					@Override
    					public String call(Tuple2<String, String> v1) throws Exception {
    						StaticHash.add((String)v1._1,(String)v1._2);
    						return null;
    					}
    		        });

    		        long finish = System.nanoTime();
    		        System.out.println("Bloc inserted after ...");
    		        System.out.println(finish - start);
    		        insert_bloc_times.add(finish - start);
    		        min_index = max_index;
    		        
    		        // search index using spark
    		        for (int k = 0; k < nb_search; k++) {
    					int search_index = ThreadLocalRandom.current().nextInt(0, max_index);
    					String search_key = keys.get(search_index)._1;
    					long start2 = System.nanoTime();
    					if (StaticHash.search(search_key) != null) {
    						if (k % 500 == 0) {
    							System.out.println("Searched key Found!");
    						}
    					} else {
    						System.out.println("Searched key do not exist!");
    					}
    					long finish2 = System.nanoTime();
    					search_bloc_times.add(finish2 - start2);

    				}

    		}
    	    sc.close();
    		PrintWriter writer = new PrintWriter(new FileWriter("bloc_Insertions_times_Spark.txt"));
    		writer.println(insert_bloc_times.toString());
    		writer.close();
    		
    		PrintWriter writer3 = new PrintWriter(new FileWriter("bloc_Search_times_Spark.txt"));
    		writer3.println(search_bloc_times.toString());
    		writer3.close();
            
    	    
    	    
    	    /*
    	    lines.foreach( new VoidFunction<Tuple2<String,String>>() {
    	
    			@Override
    			public void call(Tuple2 t) throws Exception {
    				StaticHash.add((String)t._1,(String)t._2);
    				System.out.println("Current_size at now = " + StaticHash.get_current_size());
    				}
    			} );
    	    */

    	    //System.out.println("Current_size at now = " + StaticHash.get_current_size());

    	}
    }