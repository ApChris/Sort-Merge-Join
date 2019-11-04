#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "CUnit/Basic.h"
#include "./include/relation.h"
#include "./include/quicksort.h"
#include "./include/histogram.h"
#include "./include/psum.h"
#include "./include/result.h"

// compilation method: gcc -o unit_test unit_testing.c ./src/quicksort.c ./src/histogram.c ./src/psum.c ./src/result.c -lcunit


// Test suite initializer and cleanup functions
int SortInit(void){
	return 0;
}
int SortCleanup(void){
	return 0;
}

void Init_relation(relation *rel, int size){
	if ((rel->tuples = (tuple*)malloc(size*sizeof(tuple))) == NULL)
	{
		perror("sort relation\n");
        exit(-1);
	}
	rel->num_tuples = 0;
}

// Quicksort testing: various arrays to test if they are sorted successfully
void Quick_Sort_Simple(void){
	uint64_t v[10] = {1554683260001491562, 500327316197258909, 500327316197258909, 500327316197258909,
						500327316197258909, 500327316197258909, 500327316197258909, 500327316197258909,
						500327316197258909, 500327316197258909};
	uint64_t w[10] = {5003273161972589095, 5003273161972589094, 5003273161972589093, 5003273161972589092,
						5003273161972589091, 5003273161972589091, 5003273161972589092, 5003273161972589093,
						5003273161972589094, 5003273161972589095};
	uint64_t x[10] = {155468326000149151, 155468326000149152, 155468326000149153, 155468326000149154,
						155468326000149155, 155468326000149156, 155468326000149157, 155468326000149158,
						155468326000149159, 155468326000149159};
	uint64_t y[10] = {155468326000149159, 155468326000149158, 155468326000149157, 155468326000149157,
						155468326000149156, 155468326000149155, 155468326000149154, 155468326000149153,
						155468326000149152, 155468326000149151};
	uint64_t z[10] = {5003273161972589091, 5003273161972589092, 5003273161972589093, 5003273161972589094,
						5003273161972589095, 5003273161972589095, 5003273161972589094, 5003273161972589093,
						5003273161972589092, 5003273161972589091};


	relation r1;
    relation *sort1 = &r1;
    Init_relation(sort1, 10);

    relation r2;
    relation *sort2 = &r2;
    Init_relation(sort2, 10);

    relation r3;
    relation *sort3 = &r3;
    Init_relation(sort3, 10);

    relation r4;
    relation *sort4 = &r4;
    Init_relation(sort4, 10);

    relation r5;
    relation *sort5 = &r5;
    Init_relation(sort5, 10);

    for (int i = 0; i < 10; i++)
    {
        sort1->tuples[i].key = v[i];
        sort1->tuples[i].payload = i;
        sort1->num_tuples++;

        sort2->tuples[i].key = w[i];
        sort2->tuples[i].payload = i;
        sort2->num_tuples++;

        sort3->tuples[i].key = x[i];
        sort3->tuples[i].payload = i;
        sort3->num_tuples++;

        sort4->tuples[i].key = y[i];
        sort4->tuples[i].payload = i;
        sort4->num_tuples++;

        sort5->tuples[i].key = z[i];
        sort5->tuples[i].payload = i;
        sort5->num_tuples++;
    }

    Quicksort(sort1, 0, sort1->num_tuples-1);
    Quicksort(sort2, 0, sort2->num_tuples-1);
    Quicksort(sort3, 0, sort3->num_tuples-1);
    Quicksort(sort4, 0, sort4->num_tuples-1);
    Quicksort(sort5, 0, sort5->num_tuples-1);

    for (int i = 0; i < 10; i++)
    {
    	CU_ASSERT_EQUAL(sort1->tuples[i].key, i == 9 ? 1554683260001491562 : 500327316197258909);
    	CU_ASSERT_EQUAL(sort3->tuples[i].key, x[i]);
    	CU_ASSERT_EQUAL(sort4->tuples[i].key, y[9-i]);
    }

    int offset = 4;
    for (int i = 5; i < 10; i++)
    {
    	CU_ASSERT_EQUAL(sort2->tuples[i-offset-1].key, w[offset]);
    	CU_ASSERT_EQUAL(sort2->tuples[i-offset].key, w[i]);
    	offset--;
    }

    offset = 0;
    for (int i = 10; i > 5; i--)
    {
    	CU_ASSERT_EQUAL(sort5->tuples[offset*2].key, z[i-1]);
    	CU_ASSERT_EQUAL(sort5->tuples[offset*2 + 1].key, z[offset]);
    	offset++;
    }

}

// Quicksort testing: some edge cases
void Quick_Sort_Edge(void){
	uint64_t v[1] = {500327316197258901};
	uint64_t w[2] = {500327316197258902, 500327316197258901};
	uint64_t x[2] = {500327316197258901, 500327316197258902};
	int *y = NULL;

	relation r1;
    relation *sort1 = &r1;
    Init_relation(sort1, 1);

    relation r2;
    relation *sort2 = &r2;
    Init_relation(sort2, 2);

    relation r3;
    relation *sort3 = &r3;
    Init_relation(sort3, 2);

   	sort1->tuples[0].key = v[0];
   	sort1->tuples[0].payload = 0;
   	sort1->num_tuples++;

   	for (int i = 0; i < 2; i++)
   	{
   		sort2->tuples[i].key = w[i];
		sort2->tuples[i].payload = i;
		sort2->num_tuples++;

		sort3->tuples[i].key = x[i];
		sort3->tuples[i].payload = i;
		sort3->num_tuples++;
   	}

    relation *sort4 = NULL;

    Quicksort(sort1, 0, sort1->num_tuples-1);
    Quicksort(sort2, 0, sort2->num_tuples-1);
    Quicksort(sort3, 0, sort3->num_tuples-1);
    // Quicksort(sort4, 0, 1);

    CU_ASSERT_EQUAL(sort1->tuples[0].key, 500327316197258901);
    CU_ASSERT_EQUAL(sort2->tuples[0].key, 500327316197258901);
    CU_ASSERT_EQUAL(sort2->tuples[1].key, 500327316197258902);
    CU_ASSERT_EQUAL(sort3->tuples[0].key, 500327316197258901);
    CU_ASSERT_EQUAL(sort3->tuples[1].key, 500327316197258902);
    CU_ASSERT_EQUAL(sort4, NULL);

}

// Histogram and Psum testing: regular histogram and psum creation
void Histogram_Psum_Simple(void){
	uint64_t R[20] = {9238353, 33367813, 4997225, 47348955, 23851595, 31331698,
						11715274, 49281279, 29490875, 13006865, 28013437, 7446775,
						9090272, 20872125, 35232778, 46584431, 48757687, 479600, 32860244,
						39747084};

	relation struct_rel;
    relation *rel = &struct_rel;
    Init_relation(rel, 20);

    for (int i = 0; i < 20; i++)
    {
    	rel->tuples[i].key = R[i];
    	rel->tuples[i].payload = i;
    	rel->num_tuples++;
    }

    histogram struct_h;
    histogram *hist = &struct_h;
    Histogram(rel, hist, 0, 0, 20);

    psum struct_p;
    psum *ps = &struct_p;
    Psum(hist, ps, 0);

    int result_buckets[20] = {5, 10, 12, 17, 75, 81, 84, 105, 111, 112, 114, 
    					125, 183, 187, 189, 202, 219, 224, 247, 255};
    uint64_t i = 0;
    int j=0;
    while(i < hist -> num_tuples)
    {
        if(hist -> hist_tuples[i].sum != 0){
            CU_ASSERT_EQUAL(hist->hist_tuples[i].key, result_buckets[j]);
            CU_ASSERT_EQUAL(hist->hist_tuples[i].sum, 1);

            CU_ASSERT_EQUAL(ps->psum_tuples[i].key, result_buckets[j]);
            CU_ASSERT_EQUAL(ps->psum_tuples[i].position, j);
        	j++;
        }
        i++;
    } 

}

// Histogram and Psum testing: 1st byte is the same
void Histogram_Psum_Edge(void){
    uint64_t R[20] = {251592448, 16689664, 37120, 253689600, 181775360, 14003200,
                        743424, 1006080, 1047040, 64000, 699392, 44032, 54784,
                        15682560, 751616, 900096, 1031168, 56832, 911104, 11471872};

	relation struct_rel;
    relation *rel = &struct_rel;
    Init_relation(rel, 20);

    for (int i = 0; i < 20; i++)
    {
    	rel->tuples[i].key = R[i];
    	rel->tuples[i].payload = i;
    	rel->num_tuples++;
    }

    histogram struct_h;
    histogram *hist = &struct_h;
    Histogram(rel, hist, 0, 0, 20);

    psum struct_p;
    psum *ps = &struct_p;
    Psum(hist, ps, 0);

    uint64_t i = 0;
    int j=0;
    while(i < hist -> num_tuples)
    {
        if(hist -> hist_tuples[i].sum != 0){
            CU_ASSERT_EQUAL(hist->hist_tuples[i].key, 0);
            CU_ASSERT_EQUAL(hist->hist_tuples[i].sum, 20);

            CU_ASSERT_EQUAL(ps->psum_tuples[i].key, 0);
            CU_ASSERT_EQUAL(ps->psum_tuples[i].position, j);
        	j++;
        }
        i++;
    }
}

// Join testing: Regular join of two arrays
void Join_Simple(void){

	uint64_t A[10] = {543, 123, 654, 876, 7564, 4536, 7654, 432, 1265, 45234};
	uint64_t Aid[10] = {0, 0, 0, 1, 1, 5, 5, 7, 9, 9};

	uint64_t B[6] = {435, 1346, 435312, 34543, 5345, 543};
	uint64_t Bid[6] = {0, 1, 3, 5, 7, 9};


	relation r1;
    relation *join1 = &r1;
    Init_relation(join1, 10);

    relation r2;
    relation *join2 = &r2;
    Init_relation(join2, 6);

    for (int i = 0; i < 10; i++)
    {
    	join1->tuples[i].key = Aid[i];
    	join1->tuples[i].payload = A[i];
    	join1->num_tuples++;
    }
    for (int j = 0; j < 6; j++)
    {
    	join2->tuples[j].key = Bid[j];
    	join2->tuples[j].payload = B[j];
    	join2->num_tuples++;
    }

    result *res = resultInit();

    Join(join1, join2, res);

    CU_ASSERT_EQUAL(res->buffer[0][0], 0);
    CU_ASSERT_EQUAL(res->buffer[1][0], 0);
    CU_ASSERT_EQUAL(res->buffer[2][0], 0);
    CU_ASSERT_EQUAL(res->buffer[3][0], 1);
    CU_ASSERT_EQUAL(res->buffer[4][0], 1);
    CU_ASSERT_EQUAL(res->buffer[5][0], 5);
    CU_ASSERT_EQUAL(res->buffer[6][0], 5);
    CU_ASSERT_EQUAL(res->buffer[7][0], 7);
    CU_ASSERT_EQUAL(res->buffer[8][0], 9);
    CU_ASSERT_EQUAL(res->buffer[9][0], 9);


}

// Join testing: every element is similar
void Join_Same(void){

	uint64_t A[10] = {543, 123, 654, 876, 7564, 4536, 7654, 432, 1265, 45234};
	uint64_t Aid[10] = {0, 0, 0, 1, 1, 5, 5, 7, 9, 9};

	uint64_t B[10] = {435, 1346, 435312, 34543, 5345, 543, 4325, 534532, 123124, 312424};
	uint64_t Bid[10] = {0, 0, 0, 1, 1, 5, 5, 7, 9, 9};

	relation r1;
    relation *join1 = &r1;
    Init_relation(join1, 10);

    relation r2;
    relation *join2 = &r2;
    Init_relation(join2, 10);

    for (int i = 0; i < 10; i++)
    {
    	join1->tuples[i].key = Aid[i];
    	join1->tuples[i].payload = A[i];
    	join1->num_tuples++;
    }
    for (int j = 0; j < 10; j++)
    {
    	join2->tuples[j].key = Bid[j];
    	join2->tuples[j].payload = B[j];
    	join2->num_tuples++;
    }

    result *res = resultInit();

    Join(join1, join2, res);

    CU_ASSERT_EQUAL(res->buffer[0][0], 0);
    CU_ASSERT_EQUAL(res->buffer[1][0], 0);
    CU_ASSERT_EQUAL(res->buffer[2][0], 0);
    CU_ASSERT_EQUAL(res->buffer[3][0], 0);
    CU_ASSERT_EQUAL(res->buffer[4][0], 0);
    CU_ASSERT_EQUAL(res->buffer[5][0], 0);
    CU_ASSERT_EQUAL(res->buffer[6][0], 0);
    CU_ASSERT_EQUAL(res->buffer[7][0], 0);
    CU_ASSERT_EQUAL(res->buffer[8][0], 0);
    CU_ASSERT_EQUAL(res->buffer[9][0], 1);
    CU_ASSERT_EQUAL(res->buffer[10][0], 1);
    CU_ASSERT_EQUAL(res->buffer[11][0], 1);
    CU_ASSERT_EQUAL(res->buffer[12][0], 1);
    CU_ASSERT_EQUAL(res->buffer[13][0], 5);
    CU_ASSERT_EQUAL(res->buffer[14][0], 5);
    CU_ASSERT_EQUAL(res->buffer[15][0], 5);
    CU_ASSERT_EQUAL(res->buffer[16][0], 5);
    CU_ASSERT_EQUAL(res->buffer[17][0], 7);
    CU_ASSERT_EQUAL(res->buffer[18][0], 9);
    CU_ASSERT_EQUAL(res->buffer[19][0], 9);
    CU_ASSERT_EQUAL(res->buffer[20][0], 9);
    CU_ASSERT_EQUAL(res->buffer[21][0], 9);

}

// Join testing: no similar element
void Join_Different(void){

	uint64_t A[10] = {543, 123, 654, 876, 7564, 4536, 7654, 432, 1265, 45234};
	uint64_t Aid[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

	uint64_t B[10] = {435, 1346, 435312, 34543, 5345, 543, 4325, 534532, 123124, 312424};
	uint64_t Bid[10] = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

	relation r1;
    relation *join1 = &r1;
    Init_relation(join1, 10);

    relation r2;
    relation *join2 = &r2;
    Init_relation(join2, 10);

    for (int i = 0; i < 10; i++)
    {
    	join1->tuples[i].key = Aid[i];
    	join1->tuples[i].payload = A[i];
    	join1->num_tuples++;
    }
    for (int j = 0; j < 10; j++)
    {
    	join2->tuples[j].key = Bid[j];
    	join2->tuples[j].payload = B[j];
    	join2->num_tuples++;
    }

    result *res = resultInit();
    result *printres = res;

    uint64_t counter = 0;
    counter = Join(join1, join2, res);
    CU_ASSERT_EQUAL(counter, 0);
}


int main(int argc, char const *argv[])
{
	CU_pSuite pSuite = NULL;

	// Initialize unit test registry
	if (CU_initialize_registry() != CUE_SUCCESS){
		return CU_get_error();
	}

	// Add test suite to registry
	pSuite = CU_add_suite("sorting suite", &SortInit, &SortCleanup);
	if (pSuite == NULL){
		CU_cleanup_registry();
		return CU_get_error();
	}

	// Add unit tests to the suite
	if ((CU_ADD_TEST(pSuite, Quick_Sort_Simple)==NULL) || 
		(CU_ADD_TEST(pSuite, Quick_Sort_Edge)==NULL) ||
		(CU_ADD_TEST(pSuite, Histogram_Psum_Simple)==NULL) ||
		(CU_ADD_TEST(pSuite, Histogram_Psum_Edge)==NULL) ||
		(CU_ADD_TEST(pSuite, Join_Simple)==NULL) || 
		(CU_ADD_TEST(pSuite, Join_Same)==NULL) ||
		(CU_ADD_TEST(pSuite, Join_Different)==NULL))
	{
		CU_cleanup_registry();
		return CU_get_error();
	}

	// Run all unit tests using CUnit's basic interface
	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	CU_cleanup_registry();
	return CU_get_error();
}