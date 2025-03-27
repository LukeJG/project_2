/*package project_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._
import scala.util.Random

object main{

  val seed = new java.util.Date().hashCode;
  val rand = new scala.util.Random(seed);

  class hash_function(numBuckets_in: Long) extends Serializable {  // a 2-universal hash family, numBuckets_in is the numer of buckets
    val p: Long = 2147483587;  // p is a prime around 2^31 so the computation will fit into 2^63 (Long)
    val a: Long = (rand.nextLong %(p-1)) + 1  // a is a random number is [1,p]
    val b: Long = (rand.nextLong % p) // b is a random number in [0,p]
    val numBuckets: Long = numBuckets_in

    def convert(s: String, ind: Int): Long = {
      if(ind==0)
        return 0;
      return (s(ind-1).toLong + 256 * (convert(s,ind-1))) % p;
    }

    def hash(s: String): Long = {
      return ((a * convert(s,s.length) + b) % p) % numBuckets;
    }

    def hash(t: Long): Long = {
      return ((a * t + b) % p) % numBuckets;
    }

    def zeroes(num: Long, remain: Long): Int =
    {
      if((num & 1) == 1 || remain==1)
        return 0;
      return 1+zeroes(num >> 1, remain >> 1);
    }

    def zeroes(num: Long): Int =        /*calculates #consecutive trialing zeroes  */
    {
      return zeroes(num, numBuckets)
    }
  }

  class four_universal_Radamacher_hash_function extends hash_function(2) {  // a 4-universal hash family, numBuckets_in is the numer of buckets
    override val a: Long = (rand.nextLong % p)   // a is a random number is [0,p]
    override val b: Long = (rand.nextLong % p) // b is a random number in [0,p]
    val c: Long = (rand.nextLong % p)   // c is a random number is [0,p]
    val d: Long = (rand.nextLong % p) // d is a random number in [0,p]

    override def hash(s: String): Long = {     /* returns +1 or -1 with prob. 1/2 */
      val t= convert(s,s.length)
      val t2 = t*t % p
      val t3 = t2*t % p
      return if ( ( ((a * t3 + b* t2 + c*t + b) % p) & 1) == 0 ) 1 else -1;
    }

    override def hash(t: Long): Long = {       /* returns +1 or -1 with prob. 1/2 */
      val t2 = t*t % p
      val t3 = t2*t % p
      return if( ( ((a * t3 + b* t2 + c*t + b) % p) & 1) == 0 ) 1 else -1;
    }
  }
/*
  class BJKSTSketch(bucket_in: Set[(String, Int)] ,  z_in: Int, bucket_size_in: Int) extends Serializable {
/* A constructor that requies intialize the bucket and the z value. The bucket size is the bucket size of the sketch. */

    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in

    val BJKST_bucket_size = bucket_size_in

    def this(s: String, z_of_s: Int, bucket_size_in: Int){
      /* A constructor that allows you pass in a single string, zeroes of the string, and the bucket size to initialize the sketch */
      this(Set((s, z_of_s )) , z_of_s, bucket_size_in)
    }

    def +(that: BJKSTSketch): BJKSTSketch = {    /* Merging two sketches */

    }

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {   /* add a string to the sketch */

    }
  }

  def BJKST(x: RDD[String], width: Int, trials: Int) : Double = {
   // TO-DO
    val hash = new hash_function(width)  
    
    val estimatesRDD = x.map { plate =>
      val estimates = (1 to trials).map { _ =>
        val bucket = hash(plate)
        bucket.toDouble
      }
      (plate, estimates)
    }
    val allEstimates = estimatesRDD.flatMap { case (_, estimates) => estimates }.collect().toList

    
    val sortedEstimates = allEstimates.sorted
    val mid = sortedEstimates.size / 2
    val median = if (sortedEstimates.size % 2 == 1) {
      sortedEstimates(mid)  // Odd number of elements
    } else {
      (sortedEstimates(mid - 1) + sortedEstimates(mid)) / 2.0  // Even number of elements
    }

    median

  }

  class BJKSTSketch(bucket_in: Set[(String, Int)], z_in: Int, bucket_size_in: Int) extends Serializable {
    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in
    val BJKST_bucket_size = bucket_size_in

    def this(s: String, z_of_s: Int, bucket_size_in: Int) = {
      this(Set((s, z_of_s)), z_of_s, bucket_size_in)
    }

    def +(that: BJKSTSketch): BJKSTSketch = {
  
      val combinedBucket = this.bucket ++ that.bucket
      val maxZ = math.max(this.z, that.z)
      
      val newSketch = new BJKSTSketch(combinedBucket, maxZ, this.BJKST_bucket_size)
      newSketch.shrinkBucket()
      newSketch
    }

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {
      if (z_of_s < this.z) return this
      
      val newBucket = this.bucket + ((s, z_of_s))
      val newSketch = new BJKSTSketch(newBucket, this.z, this.BJKST_bucket_size)
      newSketch.shrinkBucket()
      newSketch
    }

    private def shrinkBucket(): Unit = {
      while (bucket.size > BJKST_bucket_size) {
        z += 1
        bucket = bucket.filter(_._2 >= z)
      }
    }

    def estimate(): Long = {
      (bucket.size * math.pow(2, z)).toLong
    }
  }

  object BJKSTAlgorithm {
    def countTrailingZeros(d: Double): Int = {
      if (d == 0) return 0
      math.floor(math.log(d) / math.log(2)).toInt
    }

    def BJKST(x: RDD[String], width: Int, trials: Int): Double = {
   
      val estimates = (1 to trials).map { _ =>
        val hash = (s: String) => {
          val random = new Random(s.hashCode)
          random.nextDouble()
        }

        val sketch = x.map { s =>
          val hashVal = hash(s)
          val zeros = countTrailingZeros(hashVal)
          (s, zeros)
        }.filter(_._2 >= 0)  // Ensure we have valid elements
        .aggregate(new BJKSTSketch("", 0, width))(
          (sketch, item) => sketch.add_string(item._1, item._2),
          (sketch1, sketch2) => sketch1 + sketch2
        )

        sketch.estimate()
      }

      val sortedEstimates = estimates.sorted
      val medianIndex = sortedEstimates.length / 2
      
      if (sortedEstimates.length % 2 == 0) {
        (sortedEstimates(medianIndex - 1) + sortedEstimates(medianIndex)) / 2.0
      } else {
        sortedEstimates(medianIndex).toDouble
      }
    }
  }*/

  class BJKSTSketch(bucket_in: Set[(String, Int)] ,  z_in: Int, bucket_size_in: Int) extends Serializable {
/* A constructor that requies intialize the bucket and the z value. The bucket size is the bucket size of the sketch. */

    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in

    val BJKST_bucket_size = bucket_size_in

    def this(s: String, z_of_s: Int, bucket_size_in: Int){
      /* A constructor that allows you pass in a single string, zeroes of the string, and the bucket size to initialize the sketch */
      this(Set((s, z_of_s )) , z_of_s, bucket_size_in)
    }

    def +(that: BJKSTSketch): BJKSTSketch = {    /* Merging two sketches */
      // Handle empty bucket cases
      if (this.bucket.isEmpty) return that
      if (that.bucket.isEmpty) return this
      
      // Merge buckets
      val merged_bucket = this.bucket ++ that.bucket
      // Take the maximum z value (threshold)
      val merged_z = scala.math.max(this.z, that.z)
      
      // Filter the bucket to only include elements with zeroes >= z
      var filtered_bucket = merged_bucket.filter(_._2 >= merged_z)
      var final_z = merged_z
      
      // If bucket is still too large, increase z and filter more
      while (filtered_bucket.size > BJKST_bucket_size && !filtered_bucket.isEmpty) {
        final_z += 1
        filtered_bucket = filtered_bucket.filter(_._2 >= final_z)
      }
      
      return new BJKSTSketch(filtered_bucket, final_z, BJKST_bucket_size)
    }

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {   /* add a string to the sketch */
      // Check if the string has enough trailing zeros to be included
      if (z_of_s >= this.z) {
        // Add the string to the bucket
        val new_bucket = this.bucket + ((s, z_of_s))
        
        // If bucket is not too large, return with same z
        if (new_bucket.size <= BJKST_bucket_size) {
          return new BJKSTSketch(new_bucket, this.z, BJKST_bucket_size)
        } 
        // Otherwise, increase z and filter
        else {
          var new_z = this.z + 1
          var filtered_bucket = new_bucket.filter(_._2 >= new_z)
          
          // Keep increasing z if needed until bucket size is within limit
          while (filtered_bucket.size > BJKST_bucket_size && !filtered_bucket.isEmpty) {
            new_z += 1
            filtered_bucket = filtered_bucket.filter(_._2 >= new_z)
          }
          
          return new BJKSTSketch(filtered_bucket, new_z, BJKST_bucket_size)
        }
      } else {
        // String doesn't have enough trailing zeros, return unchanged sketch
        return this
      }
    }
    
    def estimate(): Double = {
      // Estimate the number of distinct elements
      if (bucket.isEmpty) {
        return 0.0
      }
      // F0 estimate is: |B| * 2^z
      return bucket.size * scala.math.pow(2, z)
    }
  }

def BJKST(x: RDD[String], width: Int, trials: Int) : Double = {
    // Create hash functions for each trial - use a more appropriate size for numBuckets
    // Long.MaxValue is too large and may cause inaccurate trailing zero counts
    val hash_functions = Array.fill(trials)(new hash_function(1L << 32)) // Using 2^32 instead of Long.MaxValue
    
    // Process the RDD for each trial
    val estimates = (0 until trials).map { trial =>
      // Get the hash function for this trial
      val h = hash_functions(trial)
      
      // Process the RDD with this hash function
      // Start with an empty sketch with z=0
      val initialSketch = new BJKSTSketch(Set.empty[(String, Int)], 0, width)
      
      val result = x.aggregate(initialSketch)(
        // For each string, compute its hash, count trailing zeros, and add to sketch
        (sketch, s) => {
          val hash_value = h.hash(s)
          val zeros = h.zeroes(hash_value)
          sketch.add_string(s, zeros)
        },
        // Merge sketches from different partitions
        (sketch1, sketch2) => sketch1 + sketch2
      )
      
      // Get the estimate for this trial
      val estimate = result.estimate()
      // For debugging
      println(s"Trial $trial: z=${result.z}, bucket size=${result.bucket.size}, estimate=$estimate")
      // Apply a small non-zero minimum to avoid returning exactly 0.0
      if (estimate < 1.0 && result.bucket.nonEmpty) 1.0 else estimate
    }
    
    // Return the median of the estimates
    val sorted_estimates = estimates.sorted
    if (trials % 2 == 0) {
      (sorted_estimates(trials/2) + sorted_estimates(trials/2 - 1)) / 2.0
    } else {
      sorted_estimates(trials/2)
    }
  }

  
  def tidemark(x: RDD[String], trials: Int): Double = {
    val h = Seq.fill(trials)(new hash_function(2000000000))

    def param0 = (accu1: Seq[Int], accu2: Seq[Int]) => Seq.range(0,trials).map(i => scala.math.max(accu1(i), accu2(i)))
    def param1 = (accu1: Seq[Int], s: String) => Seq.range(0,trials).map( i =>  scala.math.max(accu1(i), h(i).zeroes(h(i).hash(s))) )

    val x3 = x.aggregate(Seq.fill(trials)(0))( param1, param0)
    val ans = x3.map(z => scala.math.pow(2,0.5 + z)).sortWith(_ < _)( trials/2) /* Take the median of the trials */

    return ans
  }



/*
  def Tug_of_War(x: RDD[String], width: Int, depth:Int) : Long = {
    val hashFunction = new four_universal_Radamacher_hash_function

    def getHashesForString(s: String): Seq[Long] = {
      (0 until width * depth).map(_ => hashFunction.hash(s).toLong)
  }

    val hashedRDD: RDD[Seq[Long]] = x.map(getHashesForString)

    val meansRDD: RDD[Seq[Double]] = hashedRDD.map{ hashes =>
      val groupedHashes = hashes.grouped(width).toSeq
      groupedHashes.map(group => group.sum.toDouble / group.size)
  }

    val means: Seq[Double] = meansRDD.collect().flatten

    val sortedMeans = means.sorted
    val medianIndex = sortedMeans.size / 2
    val median = sortedMeans(medianIndex)

    median.toLong

  }
*/
  def Tug_of_War(x: RDD[String], width: Int, depth: Int): Long = {
    // Count item frequencies first
    val itemCounts = x.map(item => (item, 1L)).reduceByKey(_ + _)
    
    // Perform multiple runs of the Tug-of-War sketch
    val groupedResults = (1 to (width * depth)).map { _ =>
      // Create a new hash function for each run
      val hashFunc = new four_universal_Radamacher_hash_function()
      
      // Compute z for this run
      val z = itemCounts.map { case (item, count) => 
        count * hashFunc.hash(item)
      }.sum()
      
      z * z  // Square the result
    }
    
    // Group the results into 'width' sized groups and compute means
    val groupMeans = groupedResults.grouped(width).map { group =>
      group.sum.toDouble / width
    }.toArray
    
    // Return the median of the group means
    val sortedMeans = groupMeans.sorted
    val medianIndex = sortedMeans.length / 2
    
    // If even number of means, take the lower middle value
    sortedMeans(medianIndex).toLong
  }

  def exact_F0(x: RDD[String]) : Long = {
    val ans = x.distinct.count
    return ans
  }

  def exact_F2(x: RDD[String]) : Long = {
      return x.map(plate => (plate, 1)).reduceByKey((a, b) => a + b).map(plate_count => Math.pow(plate_count._2, 2)).sum().toLong
   }



  def main(args: Array[String]) {
 /*   val spark = SparkSession.builder().appName("Project_2").getOrCreate()

    if(args.length < 2) {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
    val input_path = args(0)

  //    val df = spark.read.format("csv").load("data/2014to2017.csv")
    val df = spark.read.format("csv").load(input_path)
    val dfrdd = df.rdd.map(row => row.getString(0))
*/
    val spark = SparkSession.builder()
      .master("local[*]")  // Use all available cores
      .appName("Project_2")
      .getOrCreate()

    // Rest of your existing main method code remains the same
    if(args.length < 2) {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
    val input_path = args(0)

    val df = spark.read.format("csv")
      .option("header", "false")  // Add this if your CSV doesn't have headers
      .load(input_path)
    val dfrdd = df.rdd.map(row => row.getString(0))


    val startTimeMillis = System.currentTimeMillis()

    if(args(1)=="BJKST") {
      if (args.length != 4) {
        println("Usage: project_2 input_path BJKST #buckets trials")
        sys.exit(1)
      }
      val ans = BJKST(dfrdd, args(2).toInt, args(3).toInt)
     // val ans = BJKSTSketch.BJKST(dfrdd, args(2).toInt, args(3).toInt)


      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("BJKST Algorithm. Bucket Size:"+ args(2) + ". Trials:" + args(3) +". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="tidemark") {
      if(args.length != 3) {
        println("Usage: project_2 input_path tidemark trials")
        sys.exit(1)
      }
      val ans = tidemark(dfrdd, args(2).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Tidemark Algorithm. Trials:" + args(2) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")

    }
    else if(args(1)=="ToW") {
       if(args.length != 4) {
         println("Usage: project_2 input_path ToW width depth")
         sys.exit(1)
      }
      val ans = Tug_of_War(dfrdd, args(2).toInt, args(3).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Tug-of-War F2 Approximation. Width :" +  args(2) + ". Depth: "+ args(3) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF2") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF2")
        sys.exit(1)
      }
      val ans = exact_F2(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F2. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF0") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF0")
        sys.exit(1)
      }
      val ans = exact_F0(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F0. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
  spark.stop()
  }
}
*/

package project_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._


object main{

  val seed = new java.util.Date().hashCode;
  val rand = new scala.util.Random(seed);

  class hash_function(numBuckets_in: Long) extends Serializable {  // a 2-universal hash family, numBuckets_in is the numer of buckets
    val p: Long = 2147483587;  // p is a prime around 2^31 so the computation will fit into 2^63 (Long)
    val a: Long = (rand.nextLong %(p-1)) + 1  // a is a random number is [1,p]
    val b: Long = (rand.nextLong % p) // b is a random number in [0,p]
    val numBuckets: Long = numBuckets_in

    def convert(s: String, ind: Int): Long = {
      if(ind==0)
        return 0;
      return (s(ind-1).toLong + 256 * (convert(s,ind-1))) % p;
    }

    def hash(s: String): Long = {
      return ((a * convert(s,s.length) + b) % p) % numBuckets;
    }

    def hash(t: Long): Long = {
      return ((a * t + b) % p) % numBuckets;
    }

    def zeroes(num: Long, remain: Long): Int = {
      if((num & 1) == 1 || remain==1)
        return 0;
      return 1+zeroes(num >> 1, remain >> 1);
    }

    def zeroes(num: Long): Int =        /*calculates #consecutive trialing zeroes  */
    {
      return zeroes(num, numBuckets)
    }
  }

  class four_universal_Radamacher_hash_function extends hash_function(2) {  // a 4-universal hash family, numBuckets_in is the numer of buckets
    override val a: Long = (rand.nextLong % p)   // a is a random number is [0,p]
    override val b: Long = (rand.nextLong % p) // b is a random number in [0,p]
    val c: Long = (rand.nextLong % p)   // c is a random number is [0,p]
    val d: Long = (rand.nextLong % p) // d is a random number in [0,p]

    override def hash(s: String): Long = {     /* returns +1 or -1 with prob. 1/2 */
      val t= convert(s,s.length)
      val t2 = t*t % p
      val t3 = t2*t % p
      return if ( ( ((a * t3 + b* t2 + c*t + b) % p) & 1) == 0 ) 1 else -1;
    }

    override def hash(t: Long): Long = {       /* returns +1 or -1 with prob. 1/2 */
      val t2 = t*t % p
      val t3 = t2*t % p
      return if( ( ((a * t3 + b* t2 + c*t + b) % p) & 1) == 0 ) 1 else -1;
    }
  }

  class BJKSTSketch(bucket_in: Set[(String, Int)] ,  z_in: Int, bucket_size_in: Int) extends Serializable {
/* A constructor that requies intialize the bucket and the z value. The bucket size is the bucket size of the sketch. */

    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in

    val BJKST_bucket_size = bucket_size_in

    def this(s: String, z_of_s: Int, bucket_size_in: Int){
      /* A constructor that allows you pass in a single string zeroes of the string and the bucket size to initialize the sketch */
      this(Set((s, z_of_s )) , z_of_s, bucket_size_in)
    }

    def +(that: BJKSTSketch): BJKSTSketch = {    /* Merging two sketches */
      // Handle empty bucket cases
      if (this.bucket.isEmpty) return that
      if (that.bucket.isEmpty) return this
      

      val merged_bucket = this.bucket ++ that.bucket
      val merged_z = scala.math.max(this.z, that.z)
      
      // Filter the bucket to only include elements with zeroes >= z
      var filtered_bucket = merged_bucket.filter(_._2 >= merged_z)
      var final_z = merged_z
      

      while (filtered_bucket.size > BJKST_bucket_size && !filtered_bucket.isEmpty) {
        final_z += 1
        filtered_bucket = filtered_bucket.filter(_._2 >= final_z)
      }
      
      return new BJKSTSketch(filtered_bucket, final_z, BJKST_bucket_size)
    }

    def add_string(s: String, z_of_s: Int): BJKSTSketch = {   /* add a string to the sketch */
      // Check if the string has enough trailing zeros to be included
      if (z_of_s >= this.z) {
        val new_bucket = this.bucket + ((s, z_of_s))
        
        // If bucket is not too large return with same z
        if (new_bucket.size <= BJKST_bucket_size) {
          return new BJKSTSketch(new_bucket, this.z, BJKST_bucket_size)
        } 
        else {
          var new_z = this.z + 1
          var filtered_bucket = new_bucket.filter(_._2 >= new_z)
          
          while (filtered_bucket.size > BJKST_bucket_size && !filtered_bucket.isEmpty) {
            new_z += 1
            filtered_bucket = filtered_bucket.filter(_._2 >= new_z)
          }
          
          return new BJKSTSketch(filtered_bucket, new_z, BJKST_bucket_size)
        }
      } else {
        return this
      }
    }
    
    def estimate(): Double = {
      if (bucket.isEmpty) {
        return 0.0
      }
      // F0 estimate is: |B| * 2^z
      return bucket.size * scala.math.pow(2, z)
    }
  }


  def tidemark(x: RDD[String], trials: Int): Double = {
    val h = Seq.fill(trials)(new hash_function(2000000000))

    def param0 = (accu1: Seq[Int], accu2: Seq[Int]) => Seq.range(0,trials).map(i => scala.math.max(accu1(i), accu2(i)))
    def param1 = (accu1: Seq[Int], s: String) => Seq.range(0,trials).map( i =>  scala.math.max(accu1(i), h(i).zeroes(h(i).hash(s))) )

    val x3 = x.aggregate(Seq.fill(trials)(0))( param1, param0)
    val ans = x3.map(z => scala.math.pow(2,0.5 + z)).sortWith(_ < _)( trials/2) /* Take the median of the trials */

    return ans
  }


  def BJKST(x: RDD[String], width: Int, trials: Int) : Double = {
    val hash_functions = Array.fill(trials)(new hash_function(1L << 32)) // Using 2^32 instead of Long.MaxValue
    
    // Process the RDD for each trial
    val estimates = (0 until trials).map { trial =>

      val h = hash_functions(trial)
      val initialSketch = new BJKSTSketch(Set.empty[(String, Int)], 0, width)
      
      val result = x.aggregate(initialSketch)(
        (sketch, s) => {
          val hash_value = h.hash(s)
          val zeros = h.zeroes(hash_value)
          sketch.add_string(s, zeros)
        },
        // Merge sketches
        (sketch1, sketch2) => sketch1 + sketch2
      )
      

      val estimate = result.estimate()
      // For debugging
      println(s"Trial $trial: z=${result.z}, bucket size=${result.bucket.size}, estimate=$estimate")

      if (estimate < 1.0 && result.bucket.nonEmpty) 1.0 else estimate
    }
    
    // Return the median of the estimates
    val sorted_estimates = estimates.sorted
    if (trials % 2 == 0) {
      (sorted_estimates(trials/2) + sorted_estimates(trials/2 - 1)) / 2.0
    } else {
      sorted_estimates(trials/2)
    }
  }


  def Tug_of_War(x: RDD[String], width: Int, depth:Int) : Long = {

    val hash_functions = Array.fill(depth, width)(new four_universal_Radamacher_hash_function())
    
    val frequencies = x.map(s => (s, 1L)).reduceByKey(_ + _).cache()
    
    // Compute width * depth sketch values
    val sketches = Array.ofDim[Long](depth, width)
    
    // For each depth and width
    for (d <- 0 until depth) {
      for (w <- 0 until width) {
        val sketch_value = frequencies.map { 
          case (s, freq) => hash_functions(d)(w).hash(s) * freq 
        }.reduce(_ + _)
        

        sketches(d)(w) = sketch_value
      }
    }
    
    // Compute means for each depth (row)
    val means = Array.ofDim[Double](depth)
    for (d <- 0 until depth) {

      val sum_of_squares = sketches(d).map(x => x * x).sum
      means(d) = sum_of_squares / width
    }
    
    // Return the median of the means (rounded to Long)
    val sorted_means = means.sorted
    val median = if (depth % 2 == 0) {
      (sorted_means(depth/2) + sorted_means(depth/2 - 1)) / 2.0
    } else {
      sorted_means(depth/2)
    }
    
    return median.round
  }


  def exact_F0(x: RDD[String]) : Long = {
    val ans = x.distinct.count
    return ans
  }


  def exact_F2(x: RDD[String]) : Long = {
    val frequencies = x.map(s => (s, 1L))
                       .reduceByKey(_ + _)
    
    val f2 = frequencies.map{ case (_, count) => count * count }
                        .reduce(_ + _)
    
    return f2
  }



  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Project_2").getOrCreate()

    if(args.length < 2) {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }
    val input_path = args(0)

  //    val df = spark.read.format("csv").load("data/2014to2017.csv")
    val df = spark.read.format("csv").load(input_path)
    val dfrdd = df.rdd.map(row => row.getString(0))

    val startTimeMillis = System.currentTimeMillis()

    if(args(1)=="BJKST") {
      if (args.length != 4) {
        println("Usage: project_2 input_path BJKST #buckets trials")
        sys.exit(1)
      }
      val ans = BJKST(dfrdd, args(2).toInt, args(3).toInt)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("BJKST Algorithm. Bucket Size:"+ args(2) + ". Trials:" + args(3) +". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="tidemark") {
      if(args.length != 3) {
        println("Usage: project_2 input_path tidemark trials")
        sys.exit(1)
      }
      val ans = tidemark(dfrdd, args(2).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Tidemark Algorithm. Trials:" + args(2) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")

    }
    else if(args(1)=="ToW") {
       if(args.length != 4) {
         println("Usage: project_2 input_path ToW width depth")
         sys.exit(1)
      }
      val ans = Tug_of_War(dfrdd, args(2).toInt, args(3).toInt)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Tug-of-War F2 Approximation. Width :" +  args(2) + ". Depth: "+ args(3) + ". Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF2") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF2")
        sys.exit(1)
      }
      val ans = exact_F2(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F2. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else if(args(1)=="exactF0") {
      if(args.length != 2) {
        println("Usage: project_2 input_path exactF0")
        sys.exit(1)
      }
      val ans = exact_F0(dfrdd)
      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000

      println("==================================")
      println("Exact F0. Time elapsed:" + durationSeconds + "s. Estimate: "+ans)
      println("==================================")
    }
    else {
      println("Usage: project_2 input_path option = {BJKST, tidemark, ToW, exactF2, exactF0} ")
      sys.exit(1)
    }

  }
}

