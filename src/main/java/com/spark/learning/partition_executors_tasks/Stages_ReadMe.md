Ah — that’s actually expected in your current program, and here’s why:

---

### 1️⃣ How Spark Stages Are Determined

* Spark splits a job into **stages** based on **shuffle boundaries**.

* **Shuffle** occurs when data needs to be **redistributed across partitions**, e.g., in operations like:

    * `reduceByKey`
    * `groupByKey`
    * `join`
    * `distinct`

* **Narrow transformations** like:

    * `map`
    * `flatMap`
    * `filter`
    * `mapPartitions`
      do **not cause a shuffle** → they are **pipelined** into the same stage.

---

### 2️⃣ Your Program

```java
long count = transactions.count();
```

* You only do:

    * `parallelize()` → creates partitions
    * `count()` → an **action**
* **No shuffle operations** are used.
* So Spark only needs **1 stage** to process all partitions.
* Each partition still becomes a **separate task** within that stage (you will see 8 tasks if `numPartitions = 8`).

---

### 3️⃣ Key Takeaways

* **Stage count ≠ partition count**

    * Stage = group of tasks between shuffles
    * Task = unit of work per partition

* In your example:

    * **1 stage** → processes all partitions
    * **8 tasks** → one per partition

* If you did something like:

```java
transactions.map(t -> new Tuple2<>(t, 1))
            .reduceByKey(Integer::sum)
            .count();
```

* Then Spark would create **2 stages**:

    1. Map tasks (per partition)
    2. Reduce tasks (shuffle happens)

---

If you want, I can **modify your current code** to show **multiple stages and shuffle behavior** so you can visually see multiple stages and their tasks in logs.

Do you want me to do that?
