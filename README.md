## 1. **Exact F2**

- **LOCAL**  
  - Time Elapsed: 19s  
  - Estimate: 8,567,966,130  

- **GCP**  
  - Time Elapsed: 109s  
  - Estimate: 8,567,966,130  

---

## 2. **Tug-of-War**

- **LOCAL**  
  - Width: 10  
  - Depth: 3  
  - Time Elapsed: 91s  
  - Estimate: 7,806,926,602  

- **GCP**  
  - Tug-of-War F2 Approximation  
  - Width: 10  
  - Depth: 3  
  - Time Elapsed: 141s  
  - Estimate: 5,385,739,287  

---

## 3. **Exact F0**

- **LOCAL**  
  - Time Elapsed: 17s  
  - Estimate: 7,406,649  

- **GCP**  
  - Time Elapsed: 92s  
  - Estimate: 7,406,649  

---

## 4. **BJKST**

- **LOCAL**  
  - Bucket Size: 25  
  - Trials: 5  
  - Time Elapsed: 21s  
  - Estimate: 8,912,896.0  

- **GCP**  
  - Bucket Size: 25  
  - Trials: 5  
  - Time Elapsed: 166s  
  - Estimate: 5,767,168.0  

---

## BJKST Algorithm Analysis:

For the BJKST algorithm, the relative error is approximately `1/√width`. We want to achieve an error of ±20%. To do this, we set up the inequality:

`1/√width ≤ 0.2`

Solving for `width`, we get:

`√width ≥ 1/0.2 = 5`

Therefore, the optimal `width` is:

`width ≥ 25`

The smallest width that meets the error requirement is **25**. This value does not depend on the actual value of F0.

---
## Overall Analysis
   - The **Exact F0** and **Exact F2** algorithms are significantly faster than the **BJKST** and **Tug-of-War** algorithms, both locally and on GCP, suggesting that the **Exact** algorithms are more optimized for performance in terms of time. Moreover, the **Exact** algorithms provide stable and consistent estimates, whereas the **Tug-of-War** and **BJKST** algorithms exhibit some variability in their estimates, particularly on GCP. Overall, the **BJKST** and **Tug-of-War** algorithms have longer runtimes with higher variation in estimates, which follows their more involved and computationally complex nature.