package project_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd._


object main{
  val width = 10
  val trials = 5
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
*/
  class BJKSTSketch(bucket_in: Set[(String, Int)], z_in: Int, bucket_size_in: Int) extends Serializable {
    
    var bucket: Set[(String, Int)] = bucket_in
    var z: Int = z_in
    val BJKST_bucket_size = bucket_size_in

    // Constructor for initializing with a single string
    def this(s: String, z_of_s: Int, bucket_size_in: Int) = {
      this(Set((s, z_of_s)), z_of_s, bucket_size_in)
    }

    // Method to merge two sketches
    def +(that: BJKSTSketch): BJKSTSketch = {
      // Combine buckets, then potentially shrink
      val combinedBucket = this.bucket ++ that.bucket
      val maxZ = math.max(this.z, that.z)
      
      // Create a new sketch with combined bucket and increased z
      val newSketch = new BJKSTSketch(combinedBucket, maxZ, this.BJKST_bucket_size)
      newSketch.shrinkBucket()
      newSketch
    }

    // Method to add a string to the sketch
    def add_string(s: String, z_of_s: Int): BJKSTSketch = {
      // If z_of_s is less than current z, return current sketch
      if (z_of_s < this.z) return this
      
      // Create a new bucket with the added string
      val newBucket = this.bucket + ((s, z_of_s))
      val newSketch = new BJKSTSketch(newBucket, this.z, this.BJKST_bucket_size)
      newSketch.shrinkBucket()
      newSketch
    }

    // Helper method to shrink the bucket if it exceeds size
    private def shrinkBucket(): Unit = {
      // While bucket is too large, increment z and remove small elements
      while (bucket.size > BJKST_bucket_size) {
        z += 1
        bucket = bucket.filter(_._2 >= z)
      }
    }

    // Method to estimate distinct elements
    def estimate(): Long = {
      (bucket.size * math.pow(2, z)).toLong
    }
  }

  object BJKSTAlgorithm {
    // Count trailing zeros in binary representation
    def countTrailingZeros(d: Double): Int = {
      if (d == 0) return 0
      math.floor(math.log(d) / math.log(2)).toInt
    }

    // BJKST main function
    def BJKST(x: RDD[String], width: Int, trials: Int): Double = {
      // Run multiple trials and collect estimates
      val estimates = (1 to trials).map { _ =>
        // Create a random hash function (simplified representation)
        val hash = (s: String) => {
          // Simple hash function that creates a double between 0 and 1
          val random = new Random(s.hashCode)
          random.nextDouble()
        }

        // Perform BJKST sketch for this trial
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

      // Return median of estimates
      val sortedEstimates = estimates.sorted
      val medianIndex = sortedEstimates.length / 2
      
      if (sortedEstimates.length % 2 == 0) {
        // If even number of trials, average the two middle values
        (sortedEstimates(medianIndex - 1) + sortedEstimates(medianIndex)) / 2.0
      } else {
        // If odd number of trials, return the middle value
        sortedEstimates(medianIndex).toDouble
      }
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

