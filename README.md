# People You May Know – Hadoop MapReduce

This project uses **Hadoop’s MapReduce framework** to implement a *“People You May Know”* recommendation system for social networks.

## Overview

The system suggests potential new friends for each user based on the number of **mutual friends** they share. It works in two stages:

1. **Mutual Friend Identification (Job 1):**
   * The mapper processes direct friendships and emits friend-of-friend relationships.
   * The reducer aggregates these relationships and counts the number of mutual friends, while filtering out existing friendships.

2. **Recommendation Aggregation (Job 2):**
   * The intermediate output is passed to a second MapReduce job.
   * For each user, candidate recommendations are ranked by mutual friend count, and the top 10 recommendations are selected.

## Input Format

The input is an **adjacency list**, with one user per line: `<User><TAB><Friends>`

- `<User>` → unique integer ID corresponding to a user  
- `<Friends>` → comma-separated list of unique IDs corresponding to that user’s friends  

Notes:  
- Friendships are **mutual (undirected edges)**. If A is a friend of B, then B is also a friend of A.  
- The provided data is consistent with this rule, i.e., there is an explicit entry for each side of every friendship.  

**Example Input:**

```
1 2,3,4
2 1,4
3 1,4
4 1,2,3
```

## Output Format

The output contains one line per user in the following format:

`<User><TAB><Recommendations>`


- `<User>` → unique ID corresponding to a user  
- `<Recommendations>` → comma-separated list of unique IDs recommended for that user  

Rules for output:  
- Recommendations are **users not already friends** with `<User>`.  
- Ordered by **decreasing number of mutual friends**.  
- If fewer than 10 second-degree friends exist, output all of them in order.  
- If a user has no possible recommendations, output an empty list.  
- Ties (same number of mutual friends) are broken by ordering numerically ascending by User ID.  

**Example Output:**

```
2 3,1
3 2,1
```

(Here, user 2 and 3 are suggested to each other with 1 mutual friend.)

## Key Learnings

* Using MapReduce to parallelize graph-based recommendation problems.  
* Designing efficient mapper/reducer pipelines for multi-stage processing.  
* Handling friendship filtering, tie-breaking, and ranking at scale.  




