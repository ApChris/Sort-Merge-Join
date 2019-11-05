---


---

<h2 id="compilation-and-execution">Compilation and Execution</h2>
<pre><code>make rebuild

./smj tiny
./smj small
./smj medium
</code></pre>
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
Histogram holds the key value and it’s frequency of appearance(sum) in the input array.<br>
Psum holds the key value and it’s position that will be placed in the final array (post-sorted array).<br>
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

