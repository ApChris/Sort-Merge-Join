
---
## Part Three

<h2 id="compilation-and-execution">Compilation and Execution</h2>
<p>Clone Sort-Merge-Join in your local repository, open terminal and type the following:<br>
<code>make all</code><br>
and execute for either small or medium datasets:<br>
<code>./smj small</code><br>
<code>./smj medium</code>(not working entirely)</p>
<p>To compile Sort-Merge-Join and unit testing seperately:<br>
<code>make smj</code><br>
<code>make unit_test</code></p>
<p>To remove all object files:<br>
<code>make clean</code></p>
<p>To remove all object files and compile them all together:<br>
<code>make rebuild</code></p>

Query Optimization
---
A query can be executed in many different ways. Reordering the predicates, executing filters first etc. 
Let's say we have the following query:`A.a=B.a & B.a=C.a & C.a=D.a`
A way to execute the above query is (((A&B)&C)&D), which means that A.a=B.a would be executed first, then B.a=C.a and finally C.a=D.a.
Another way to execute the same query is (((C&B)&A)&D), which means that B.a=C.a would be executed first, then A.a=B.a and finally C.a=D.a.

The orded in which the predicates are going to be executed is very crucial to the execution time of the query. We've already decided (from Part Two) that filters have priority, in order to cut large loads of data and reduce the execution time of the Joins.

One way to evaluate the result of a predicate is going to be through the calculations of some statistics. 
## Statistics
For every column of a relation, we are going to store the following four values

 - la: minimum value of column A
 - ua: maximum value of column A
 - fa: total number of values of column A
 - da: total distinct values of column A

To, approximetaly, evaluate the distinct values of a column is going to be with the use of a boolean array of size ua - la + 1, initialized as false. For every value (x) of that particular column, we can turn the (x - ua) position of the array to true.

## Evaluation of Cardinality
We are using the above statistics, that are stored in memory, to evaluate the cardinality of every predicate's result. To do that, we made the following compromises 

<h2 id="part-two">Part Two</h2>
<h2 id="compilation-and-execution">Compilation and Execution</h2>
<p>Clone Sort-Merge-Join in your local repository, open terminal and type the following:<br>
<code>make all</code><br>
and execute for either small or medium datasets:<br>
<code>./smj small</code><br>
<code>./smj medium</code>(not working entirely)</p>
<p>To compile Sort-Merge-Join and unit testing seperately:<br>
<code>make smj</code><br>
<code>make unit_test</code></p>
<p>To remove all object files:<br>
<code>make clean</code></p>
<p>To remove all object files and compile them all together:<br>
<code>make rebuild</code></p>
<p>To execute unit testing, after compiling for unit_test:<br>
<code>./unit_test</code></p>
<h2 id="adjustments-of-part-one">Adjustments of Part One</h2>
<p>In Part One, every relations had an array of tuples{key, payload}. However, in Part Two, where we had to execute more realistic SQL queries, we needed the “payload” to change from uint64_t to an array of uint64_t. The reason behind this adjustment was the existence of an intervening struct, which holded the result of each predicate.</p>
<h2 id="preprocessing-of-datasets">Preprocessing of Datasets</h2>
<p>There are two workloads given to us: small and medium workload. The type of files found there are:</p>
<ul>
<li>r0, r1, … , r13 -&gt; binary files (our dataset)</li>
<li>.init -&gt; files which hold every name of every binary file we need (the above files)</li>
<li>.result -&gt; files which hold the desired results of each query</li>
<li>.work -&gt; files which hold the queries needed to be executed</li>
<li>.tbl files -&gt; decimal representation of r0, r1, … , r13 files</li>
</ul>
<p><strong>Metadata structure</strong><br>
In order to make use of our dataset, we had to convert the binary files into decimal and keep these in memory. So the metadata structure looks like this:</p>
<p><img src="https://scontent.fath3-4.fna.fbcdn.net/v/t1.15752-9/78893511_1215494368657842_6847555441929486336_n.jpg?_nc_cat=108&amp;_nc_ohc=-qjSB8LrSBMAQnsxAejbzptjonugnl0zYTCJviuUbGOimjXpj9Dwl7RmQ&amp;_nc_ht=scontent.fath3-4.fna&amp;oh=07fc34fff42b601929f8f7c96ea12d58&amp;oe=5E73C67B" alt="enter image description here"><br>
To start with, there is the vertical structure which is an array. This array holds the characteristics (rows, columns) of rX file and an array of pointers, each of them pointing in a certain place in memory where we allocate each of the given rX’s columns.<br>
i.e. The first node in the image above holds the characteristics of r0, which has 3 columns and 100 rows. The first pointer in r0’s array, points to the values of it’s 1st column. The second pointer to it’s 2nd column, etc.<br>
Therefore, the metadata structure holds everything we need in memory, our entire dataset.</p>
<h2 id="query">Query</h2>
<p>An example of an SQL query is something like that:</p>
<pre><code>SELECT SUM("0".c0), SUM("1".c1) 
FROM r0 "0", r2 "1", r4 "2" 
WHERE 0.c1=1.c2 and 1.c0=2.c1 and 0.c1&gt;3000
</code></pre>
<p>In .work files, though, we are given our queries in a bit different format:</p>
<pre><code>0 2 4|0.1=1.2&amp;1.0=2.1&amp;0.1&gt;3000|0.0 1.1
</code></pre>
<p>This is the same query as the above SQL query.<br>
We now demonstrate the steps needed to execute a query, just like the above.</p>
<p>We first parse through the whole query, storing every bit of information. We store:</p>
<ul>
<li>Parameters: “0 2 4”</li>
<li>Predicates: “0.1=1.2&amp;1.0=2.1”</li>
<li>Filters: “0.1&gt;3000”</li>
<li>Selects: “0.0 1.1”</li>
</ul>
<p>We prioritize our program to first execute the filters. Filtering cuts off too many values, which is very important. Not doing so would cost memory and run time the time we execute the predicates.</p>
<p><em>In this step, its mandatory to present the <strong>Intervening</strong> structure.<br>
Intervening structure holds an array of identifiers and a relation.</em></p>
<p>In our example, the array of identifiers holds values such as: 0, 1, 2. There are the distinct values of each predicate, that refer to the parameters: 0 refers to 0, 1 refers to 2, 2 refers to 4.<br>
The adjustments that we had to make from Part One to Part Two were due to this particular structure.<br>
A relation is an array of tuples, where every tuple has a key and an array of payloads. This array of payloads stores values that represent the payload of the key in i-th file.</p>
<p><strong>First step</strong><br>
After parsing, we store the filtered relation in memory and proceed to execute the predicates.</p>
<pre><code>0.1=1.2
</code></pre>
<p>0.1: relation r0 (which we have access from the <strong>metadata</strong> structure)<br>
1.2: relation r2 (same as above)<br>
The important step here is to check if any of those 2 relations have already been filtered or joined.  Follow the priority:</p>
<ol>
<li>Check if relation has already been filtered</li>
<li>Check if relation has already been stored into intervening struct</li>
</ol>
<p>We see that r0 has already been filtered, we have stored it’s results in memory. So we do NOT get r0’s values from metadata, but from memory. This little step here is extremely important.<br>
<em>After using a filter, we remove it, so that we know which filter has been used and not use it again.</em></p>
<p>We also check the intervening struct, but we see that is empty and thus we create the intervening struct.</p>
<p>The relation of intervening struct looks like this, after the first predicate:</p>

<table>
<thead>
<tr>
<th>Key</th>
<th>Payload0</th>
<th>Payload1</th>
</tr>
</thead>
<tbody>
<tr>
<td>3500</td>
<td>145</td>
<td>6745</td>
</tr>
<tr>
<td>3753</td>
<td>5</td>
<td>534</td>
</tr>
<tr>
<td>6045</td>
<td>745</td>
<td>16</td>
</tr>
</tbody>
</table><p>This means that in the 145th row of r0 and in its 2nd column (remember 0.1), we find the key to be 3500. In 6745th row of r1 and in its 3rd column (1.2), we find the key to be 3500 as well.</p>
<p><strong>Second step</strong><br>
After the first predicate, we move to the next one. The next predicate is:</p>
<pre><code>1.0=2.1
</code></pre>
<p>We have to join the 1st column of r2 with the 2nd column of r4. Follow the priority:</p>
<ol>
<li>Check if relation has already been filtered</li>
<li>Check if relation has already been stored into intervening struct</li>
</ol>
<p>There is no filter that refer to r2, so we move on to check if it exists in the intervening struct, where it does exist!<br>
The formula here is to take the entire relation of the intervening struct, and for those payloads that refer ONLY to r2 (which are the payloads under “Payload 1”), we search through the <strong>metadata</strong> structure to find the particular keys that appear in those payloads for that particular column (in our case, column 1). We then procceed to change intervening’s relation keys, into the new ones found.<br>
<em>A more detailed view of this is needed:</em> we grab the value 6745 and we search in the metadata structure to find the place where we allocated the columns of r2. After that, we already know in which column to search, column 1. We go directly to row 6745 and grab the key. We then change the key 3500 to, lets say, 34. We also do this for payload 534 and 16.</p>
<p>Finally, we have the following intervening’s relation:</p>

<table>
<thead>
<tr>
<th>Key</th>
<th>Payload0</th>
<th>Payload1</th>
</tr>
</thead>
<tbody>
<tr>
<td>34</td>
<td>145</td>
<td>6745</td>
</tr>
<tr>
<td>745</td>
<td>5</td>
<td>534</td>
</tr>
<tr>
<td>8543</td>
<td>745</td>
<td>16</td>
</tr>
</tbody>
</table><p>But we’re not finished yet. Next up we have to determine what does 2.1 mean.<br>
We search through filters, but there’s nothing there. We, then, search through intervening but there’s also nothing there. We grab the 2nd column of r4 from metadata and we hold this relation in memory!<br>
Finally, it’s time to join 2.1 with the intervening’s relation.</p>

<table>
<thead>
<tr>
<th>Key</th>
<th>Payload0</th>
<th>Payload1</th>
<th>Payload2</th>
</tr>
</thead>
<tbody>
<tr>
<td>34</td>
<td>145</td>
<td>6745</td>
<td>5</td>
</tr>
<tr>
<td>34</td>
<td>145</td>
<td>6745</td>
<td>1978</td>
</tr>
<tr>
<td>34</td>
<td>145</td>
<td>6745</td>
<td>4123</td>
</tr>
<tr>
<td>745</td>
<td>5</td>
<td>534</td>
<td>76</td>
</tr>
<tr>
<td>8543</td>
<td>745</td>
<td>16</td>
<td>133</td>
</tr>
<tr>
<td>8543</td>
<td>745</td>
<td>16</td>
<td>6438</td>
</tr>
</tbody>
</table><p><strong>Final Step</strong><br>
Finally, given Selects, we sum up the keys and exporting the results.<br>
In our example, selects needed are “0.0 1.1”.</p>
<ul>
<li>0.0: take every payload from intervening’s relation Payload0 and sum up the keys found in metadata’s r0 and column 1 for that particular payload.</li>
<li>1.1: take every payload from intervening’s relation Payload1 and sum up the keys found in metadata’s r2 and column 2 for that particular payload.</li>
</ul>
<h2 id="part-one">Part One</h2>
<h2 id="compilation-and-execution">Compilation and Execution</h2>
Clone Sort-Merge-Join in your local repository, open terminal and type the following:
<pre><code>make rebuild
</code></pre><p>Execution commands:<br>
./smj tiny<br>
./smj small<br>
./smj medium<br>
<br>
Results for tiny dataset should be 640.000, for small 50.000.000 and for medium 900.000.000 (works, but the process always gets killed, we need ~24GB ram memory, cannot compress memory space)</p>
<h2 id="sort-merge-join">Sort Merge Join</h2>
<p>Sort-Merge Join is a join algorithm, which sorts the relations by a join attribute and then merges the elements of the sorted arrays.</p>
<p>The key idea of the sort-merge algorithm is to first sort the relations by the join attribute.  Sorting the input is the most expensive part of performing a sort-merge join.</p>
<p>The method that was implemented in this project in order to sort these arrays is <strong>radix sort</strong>, starting from the most significant value.</p>
<p>The next and final stage of the Sort-Merge Join algorithm is to join the sorted arrays by their join attribute (key value).</p>
<h2 id="radix-sort-method">Radix Sort Method</h2>
<ol>
<li>
<p>For each byte i, where i varies from most significant byte to the least significant byte, push that relation to that particular bucket (bucket: [0,255]).</p>
</li>
<li>
<p>If a bucket exceeds L1-cache memory, split the bucket based on step one (i+1), else quicksort the bucket. (Splitting a bucket may occur up to 8 times)</p>
</li>
</ol>
<p>The corresponding data will be segmented to 2^8 buckets, where each bucket size must be equal to processor’s L1-cache (for this project, L1-cache equals to 64KB).</p>
<h2 id="histogram-and-psum">Histogram and Psum</h2>
<p>Two helpful structures that each serves a particular puprose on the distribution of the dataset into the buckets, making new histogram and psum structures in each iteration of the radix sort.<br>
<strong>Histogram</strong> holds the key value and it’s frequency of appearance(sum) in the input array.<br>
<strong>Psum</strong> holds the key value and it’s position that will be placed in the final array (post-sorted array).<br>
i.e.</p>
<table>
<thead>
<tr>
<th>key_value</th>
<th>Histogram’s sum</th>
<th>Psum’s pos</th>
</tr>
</thead>
<tbody>
<tr>
<td>5</td>
<td>10</td>
<td>0</td>
</tr>
<tr>
<td>8</td>
<td>15</td>
<td>10</td>
</tr>
<tr>
<td>18</td>
<td>5</td>
<td>25</td>
</tr>
</tbody>
</table><h2 id="compilation-and-execution-of-unit-testing">Compilation and Execution of unit testing</h2>
<p>Important to note that in order to compile and execute the unit testing file, you have to install the <a href="http://cunit.sourceforge.net/">CUnit library</a></p>
<pre><code>gcc -o unit_test unit_testing.c ./src/quicksort.c ./src/histogram.c ./src/psum.c ./src/result.c -lcunit
</code></pre>


<!--stackedit_data:
eyJoaXN0b3J5IjpbLTE5Mzk0ODA3MDIsLTE1NDk1MTI4NF19
-->