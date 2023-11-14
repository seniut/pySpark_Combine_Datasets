# Data Integration Project

_**main.py** is the file with solution_

## Overview
This project involves integrating datasets from three distinct sources: **Facebook**, **Google**, and a **website**. 

**sources** folder in project.

The integration is performed using a PySpark script that executes data cleaning, column renaming, data joining, and data coalescence.

## Data Joining Strategy

### 1. Join Column: `hash_key`
The column used for joining the datasets is **`hash_key`**. This key is generated using a hash function that concatenates and hashes cleaned values of `company_name`, `country_name`, and `city` from each dataset. 
Each column has been cleaned before hashing (__def key_cleaner(column)__ function)

**Reason for Choice:**
- **Uniqueness and Consistency:** Hashing these fields together provides a unique identifier that is likely to be consistent across different datasets, assuming these fields are reliable and standardized.
- **Reduction of False Matches:** Using multiple fields reduces the chances of false matches compared to using a single field.
- **Partitioning by _hash_key_:** Repartition DF by _hash_key_ for future join

### 2. Resolving Data Conflicts
In the event of data conflicts after joining:
- **Preferential Source:** Establish a hierarchy of trustworthiness among the sources. For example, prioritize Facebook data over Google or website data if it's considered more reliable.
- **Data Freshness:** Use timestamps or other indicators to choose the most recent data.
- **Completeness:** Prefer the source which provides the most complete information for the field in question.

In the current situation, preference was given to Google data. Because data looks more consistent. However, the solution includes the ability to coalesce data from other sources if data from Google is NULL.

**Reason for Approach:**
- **Source Reliability:** Different sources may have varying levels of accuracy and timeliness. And If one is empty (or NULL) we can switch to another.
- **Recency of Information:** More recent data is often more accurate. In our situation, we don't have any timestamps, so this is why we pay attention to consistency and completeness of data.
- **Completeness:** More complete information is generally more useful for analysis and decision-making. This is our situation

### 3. Handling Similar Data
When encountering similar data across sources:
- **Concatenation:** For categorical fields like `category`, I concatenate the values from different sources, separated by a delimiter (e.g., " | "). But for future analysis may be better to store each column separately. I make a concatenation just for visibility.
- **Coalescing Values:** For fields like `address`, `country_name`, `city`, etc., use coalescing to keep the first non-null value in the order of source preference.

**Reason for Approach:**
- **Richness of Data:** Concatenating categorical fields provides a comprehensive view.
- **Priority to Completeness and Reliability:** Coalescing ensures that the most complete and reliable information is retained, based on the established source hierarchy.


_The result of joining data will be stored in **destination** folder. 
Each partition into a separate file_

---
