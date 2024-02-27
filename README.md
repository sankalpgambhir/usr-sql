## usr-sql

WIP translation of SQL queries to U-Semiring expressions with subsexpressions witexpressions with subsequentexpressions with subsequentexpressions with subsequenth subsequentequent AST rewrites.


\t -> t.left = t1 /\ t.right = t2 R1[t1] R1[t2]

select * from t1

\t -> t = t1 /\ R[t1]

select * from t1, t2

(select * from t1 JOIN select * from t2) JOIN select * from t3

R1 a c

      .

   .      .

a    c   a   c

=== [a, c, a, c]

[Int, String, Bool, Int]

Int     String Bool   Int

Int String     Bool Int

SELECT t1.a t1.c, t2.a, t2.c

a b c d

a  
  b  
    c d
   .
 .    .
a b  c d

a * b * c * d

        .
  .           .
Int bool  bool char

select * from R as t1, P as t2

P = (Int, Bool, Char, Char)
P = "line like"
t2 = any tree representation of P

Tree representation of join:
\t -> sum t1 t2. R[t1] x P[t2] x t.left = t1 x t.right = t2.right

```sql
SELECT t1.*, t2.* from R as t1, P as t2
```

List representation:
\t -> sum t1 t2. R[t1] x P[t2] x t = concat(t1, t2)
t1 ci1
t2 ci2
t.ci1 = t1.ci x t.ci2 = t2.ci

- checks all columns, then replace by *

1. assume all tables have the structure above
   no transformations change the schema
2. store the base structure of every table
   on transformation, modify this structure <- slightly hard
3. somehow figure out from query? impossible?
