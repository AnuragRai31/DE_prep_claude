# Coding Reference — Databricks DE Interviews

---

## THE ONE RULE
**Always `import pyspark.sql.functions as f`. Never import functions directly — it shadows Python built-ins (`sum`, `max`, etc.).**

```python
import pyspark.sql.functions as f          # CORRECT
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import sum      # WRONG — shadows Python's sum()
```

---

## PySpark — Core Operations

### Filter
```python
df.filter("status = 'Active'")
df.filter(f.col("salary") > 50000)
df.filter((f.col("dept") == "Eng") & (f.col("salary") > 50000))
df.filter(f.col("col").isNull())
df.filter(f.col("col").isNotNull())
```

### Select + Rename + Cast
```python
df.select(
    f.col("cust_id").alias("customer_id"),
    f.col("rev").cast("double").alias("revenue"),
    f.col("hire_date").cast("date")
)
```

### Add / Replace Columns
```python
df.withColumn("tax", f.col("salary") * 0.3)
df.withColumn("status",
    f.when(f.col("salary").isNull(), "Unknown")   # isNull FIRST
    .when(f.col("salary") > 85000, "High")
    .when(f.col("salary") >= 65000, "Mid")
    .otherwise("Low")
)
```

### Dedup
```python
df.distinct()
df.dropDuplicates(["customer_id"])
df.dropDuplicates(["customer_id", "date"])
```

### Null Handling
```python
df.fillna(0, subset=["salary"])
df.fillna({"salary": 0, "name": "Unknown"})
df.dropna(subset=["customer_id"])
f.coalesce(f.col("col1"), f.col("col2"))   # first non-null value
```

---

## GroupBy + Agg

```python
df.groupBy("dept").agg(
    f.sum("salary").alias("total"),
    f.avg("salary").alias("avg"),
    f.count("emp_id").alias("headcount"),
    f.countDistinct("role").alias("unique_roles"),
    f.max("salary").alias("top_salary"),
    f.min("salary").alias("min_salary")
)
```

**Filter after groupBy → use `.filter()` not `.where()` (both work, `.filter()` is more common in PySpark)**
```python
df.groupBy("dept").agg(f.avg("salary").alias("avg_salary")) \
  .filter(f.col("avg_salary") > 75000) \
  .orderBy(f.col("avg_salary").desc())
```

---

## Joins

```python
# Same column name
df_a.join(df_b, on="customer_id", how="inner")
df_a.join(df_b, on=["customer_id", "date"], how="left")

# Different column names
df_a.join(df_b, df_a["order_id"] == df_b["return_id"], how="left")

# Anti join — rows in LEFT with NO match in right
df_orders.join(df_returns, on="order_id", how="left_anti")

# Broadcast join — when one table is small
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), on="id", how="inner")
```

**Critical rule: filter BEFORE .select() — never drop a column you still need to filter on.**
```python
# WRONG — rank is dropped before filter uses it
df.withColumn("rank", f.rank().over(w)) \
  .select("name", "dept") \
  .filter(f.col("rank") == 1)  # AnalysisException

# CORRECT
df.withColumn("rank", f.rank().over(w)) \
  .filter(f.col("rank") == 1) \
  .select("name", "dept")
```

---

## Window Functions

```python
from pyspark.sql.window import Window

# ROW_NUMBER — latest record per group (dedup / SCD2 current row)
w = Window.partitionBy("customer_id").orderBy(f.col("date").desc())
df.withColumn("rn", f.row_number().over(w)).filter("rn = 1")

# RANK — with gaps on ties (1, 1, 3)
w = Window.partitionBy("dept").orderBy(f.col("salary").desc())
df.withColumn("rank", f.rank().over(w))

# DENSE_RANK — no gaps on ties (1, 1, 2)
df.withColumn("dense_rank", f.dense_rank().over(w))

# Running total
w = Window.partitionBy("dept").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
df.withColumn("running_total", f.sum("amount").over(w))

# LAG / LEAD — compare to previous / next row
w = Window.partitionBy("customer_id").orderBy("date")
df.withColumn("prev_amount", f.lag("amount", 1).over(w))
df.withColumn("next_amount", f.lead("amount", 1).over(w))

# Salary as % of department total — no orderBy needed for partition-only aggregation
w = Window.partitionBy("dept")
df.withColumn("pct_of_dept",
    f.round(f.col("salary") / f.sum("salary").over(w) * 100, 2)
)
```

---

## Date Operations

```python
f.current_date()
f.datediff(f.col("end_date"), f.col("start_date"))    # days between
f.add_months(f.current_date(), -6)                    # 6 months ago — use this, not 365*6
f.add_months(f.current_date(), -36)                   # 3 years ago
f.date_format(f.col("date"), "yyyy-MM")               # format to string
f.to_date(f.col("date_str"), "yyyy-MM-dd")            # parse string to date
f.year(f.col("date"))
f.month(f.col("date"))
```

**Never use `365 * N` for year calculations — use `add_months(-N*12)` to handle leap years.**

---

## Delta Merge (Upsert)

```python
from delta.tables import DeltaTable

DeltaTable.forName(spark, "catalog.schema.table") \
    .alias("t") \
    .merge(df.alias("s"), "t.id = s.id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# Selective column update
DeltaTable.forName(spark, "catalog.schema.table") \
    .alias("t") \
    .merge(df.alias("s"), "t.id = s.id") \
    .whenMatchedUpdate(set={"salary": "s.salary", "updated_at": "s.updated_at"}) \
    .whenNotMatchedInsertAll() \
    .execute()
```

---

## File Operations

### Reading
```python
# CSV
df = spark.read.csv("path/", header=True, inferSchema=True)
df = spark.read.option("header", True).option("sep", "|").csv("path/")

# Parquet
df = spark.read.parquet("path/")

# Delta
df = spark.read.format("delta").load("path/")
df = spark.table("catalog.schema.table")

# Explicit schema (always use in production, never inferSchema)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
df = spark.read.csv("path", schema=schema, header=True)

# Malformed row handling
spark.read.option("mode", "FAILFAST").csv("path")       # fail on bad row
spark.read.option("mode", "DROPMALFORMED").csv("path")  # drop bad rows
spark.read.option("mode", "PERMISSIVE").csv("path")     # null bad fields (default)
```

### Writing
```python
df.write.format("delta").mode("overwrite").saveAsTable("catalog.schema.table")
df.write.format("delta").mode("append").saveAsTable("catalog.schema.table")
df.write.partitionBy("year", "month").format("delta").mode("overwrite").save("path/")
df.coalesce(1).write.csv("path/", header=True)   # single file output
```

### Write Modes
```python
mode("overwrite")   # replace entire table
mode("append")      # add rows
mode("ignore")      # do nothing if exists
mode("error")       # throw if exists (default)
```

---

## Schema Operations

```python
df.printSchema()                    # inspect types + nullability
df.dtypes                           # list of (column, type) tuples
df.columns                          # list of column names

# Cast all columns to match a defined schema
df.select([f.col(c).cast(schema[c].dataType) for c in schema.names])
```

---

## Union

```python
from functools import reduce

# Union two DataFrames
df_a.union(df_b)                                         # must have same schema
df_a.unionByName(df_b, allowMissingColumns=True)         # align by name, fill missing with null

# Union multiple DataFrames
reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), [df1, df2, df3, df4])
```

---

## Temp Views (bridge to SQL)

```python
df.createOrReplaceTempView("my_table")
spark.sql("SELECT * FROM my_table WHERE salary > 50000")
```

---

## Python Data Structures

### Decision Tree
```
Count frequency?           → Counter
Group things by key?       → defaultdict(list)
Find duplicates/unique?    → set
Transform / filter list?   → list comprehension
Fast lookup by key?        → dict
Top-N?                     → Counter.most_common(N) or sorted()[-N:]
```

---

### List

```python
lst = [1, 2, 3]

# Access
lst[0]          # first
lst[-1]         # last
lst[1:4]        # slice — end exclusive
lst[::-1]       # reverse

# Modify
lst.append(4)
lst.extend([5, 6])
lst.pop()               # remove + return last
lst.remove(2)           # remove first occurrence of value

# Sort
sorted(lst, reverse=True)                        # new list
sorted(lst, key=lambda x: x[1])                 # by second element
lst.sort()                                       # in-place

# Comprehensions — most important pattern
[x**2 for x in lst]                             # transform
[x for x in lst if x > 0]                       # filter
[x if x > 0 else 0 for x in lst]               # transform with fallback
[x for row in matrix for x in row]              # flatten 2D

# RULE: if at END = filter. if/else BEFORE for = transform.
```

**Dedup preserving order:**
```python
list(dict.fromkeys(lst))    # preserves order
list(set(lst))              # does NOT preserve order
```

---

### Dict

```python
d = {"a": 1, "b": 2}

d["a"]              # KeyError if missing
d.get("z", 0)       # safe — returns default
d["c"] = 3          # add/update
del d["a"]

for k, v in d.items():  print(k, v)

# Count frequency
freq = {}
for x in lst:
    freq[x] = freq.get(x, 0) + 1

# Sort by value
sorted(d.items(), key=lambda x: x[1], reverse=True)

# Invert
{v: k for k, v in d.items()}

# Merge two dicts
{**d1, **d2}
```

---

### Set

```python
s = {1, 2, 3}
s.add(4)
s.discard(10)       # no error if missing
"x" in s            # O(1) lookup

a = {1, 2, 3, 4}
b = {3, 4, 5, 6}
a & b               # {3, 4}     — in both
a | b               # all        — in either
a - b               # {1, 2}     — in a not b
a ^ b               # {1,2,5,6}  — in one but not both
```

---

### String

```python
s = "  Hello, World!  "

s.strip().lower()               # clean first — always
s.split(",")
" ".join(["a", "b", "c"])      # list to string
s.replace(",", "")
s[::-1]                         # reverse
s.startswith("He")
s.isdigit()
```

**Almost every string problem: `.lower()` → `.strip()` → `.split()` first.**

---

### Collections

```python
from collections import Counter, defaultdict

# Counter
Counter(["a","b","a","c","a"])      # Counter({'a':3,'b':1,'c':1})
Counter("mississippi")
c.most_common(3)                    # top 3 most frequent

# defaultdict — grouping without KeyError
groups = defaultdict(list)
for name, dept in data:
    groups[dept].append(name)

totals = defaultdict(int)
for name, amt in transactions:
    totals[name] += amt
```

---

## Common DE Interview Problems — Solutions

### Top N by aggregation
```python
# PySpark
df.groupBy("customer_id") \
  .agg(f.sum("amount").alias("total")) \
  .orderBy(f.col("total").desc()) \
  .limit(3)

# Pandas
df.groupby("customer_id")["amount"].sum() \
  .reset_index() \
  .sort_values("amount", ascending=False) \
  .head(3)
```

### Find rows with no match (anti-join)
```python
# PySpark
df_orders.join(df_returns, on="order_id", how="left_anti")

# Pandas
merged = df_orders.merge(df_returns, on="order_id", how="left", indicator=True)
merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)
```

### Deduplicate — keep latest
```python
# PySpark
w = Window.partitionBy("customer_id").orderBy(f.col("updated_at").desc())
df.withColumn("rn", f.row_number().over(w)).filter("rn = 1").drop("rn")

# Pandas
df.sort_values("updated_at", ascending=False).drop_duplicates(subset=["customer_id"])
```

### Word frequency
```python
from collections import Counter
Counter(sentence.lower().replace(",","").split())
```

### Group + sum by key
```python
from collections import defaultdict
totals = defaultdict(int)
for name, amt in [("Alice",50),("Bob",30),("Alice",20)]:
    totals[name] += amt
```

### Second largest
```python
second = sorted(set(nums))[-2]
```

### Check anagram
```python
Counter("listen") == Counter("silent")   # True
```

### Check palindrome
```python
s == s[::-1]
```

---

## Pandas Quick Reference

```python
import pandas as pd

df = pd.read_csv("file.csv")
df.head(); df.shape; df.dtypes; df.isnull().sum()

# Filter
df[df["status"] == "Active"]
df[(df["salary"] > 50000) & (df["dept"] == "Eng")]

# GroupBy
df.groupby("dept").agg(avg=("salary","mean"), n=("emp_id","count")).reset_index()

# Merge
pd.merge(df1, df2, on="id", how="left")

# Null handling
df["salary"].fillna(0, inplace=True)
df.dropna(subset=["emp_id"])

# Apply
df["band"] = df["salary"].apply(lambda x: "High" if x > 80000 else "Low")

# Window equivalent
df["rn"] = df.sort_values("salary", ascending=False).groupby("dept").cumcount() + 1

# Null rows
df[df.isnull().any(axis=1)]

# loc vs iloc
df.loc[0, "name"]       # label-based
df.iloc[0, 1]           # position-based

# Read large file in chunks
for chunk in pd.read_csv("file.csv", chunksize=10000):
    process(chunk)
```
