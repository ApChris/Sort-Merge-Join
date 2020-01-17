#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "CUnit/Basic.h"
#include "./include/getColumn.h"
#include "./include/initArray.h"
#include "./include/reorderedColumn.h"
#include "./include/quicksort.h"
#include "./include/splitBucket.h"
#include "./include/result.h"
#include "./include/sortMergeJoin.h"
#include "./include/processRelation.h"
#include "./include/intervening.h"
#include "./include/relation.h"
#include "./include/histogram.h"
#include "./include/psum.h"
#include "./include/work.h"
#include "./include/executeQuery.h"


// Test suite initializer and cleanup functions
int TestInit(void){
	return 0;
}
int TestCleanup(void){
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

// Scan testing for this particular query:  1 0|0.0=1.1&0.0=1.2&0.0<4450&1.1<4450
void Scan_Simple(void){
	uint64_t num_rows;
    statistics * stats;
    metadata *md = Read_Init_Binary("workloads/small/small.init","workloads/small/",&num_rows,stats);

    relation * relation_A = Create_Relation(md,1,0);
    relation_A = Radix_Sort(relation_A);

    relation_A = Filter(relation_A,4450,'<');

    relation * relation_B = Create_Relation(md,0,1);
    relation_B = Radix_Sort(relation_B);
    relation_B = Filter(relation_B,4450,'<');

    intervening * interv_final = interveningInit();

    Join_v2(interv_final, relation_A, relation_B, 1, 0);

    relation_B = Update_Predicates(interv_final->final_rel, 1);
    Update_Relation_Keys(md,0,2,relation_B,0);
    relation_B = Radix_Sort(relation_B);

    relation_B->tuples[0].key = 4403;
    relation_B->tuples[0].payload[0] = 777;

    // scan with relB
    if(!Scan(interv_final, interv_final -> final_rel, relation_B, 0, 1))
    {
        printf("None Results\n" );
        return;
    }


    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].key, 4403);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[0], 1439);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[1], 777);


}

// Join_v2 testing for this particular query: 3 0 1|0.2=1.0&0.2<2
void Join_v2_Simple(void){
		uint64_t num_rows;
    statistics * stats;
    metadata *md = Read_Init_Binary("workloads/small/small.init","workloads/small/",&num_rows,stats);
    // 3 0 1|0.2=1.0&0.1=2.0&0.2<2|1.2 0.1

    // r3.2
    relation * relation_A = Create_Relation(md,3,2);

    // Radix sort to r3.2
    relation_A = Radix_Sort(relation_A);

    // r3.2 < 2
    relation_A = Filter(relation_A, 2, '<');

    // Print_Relation_2(relation_A);

    // r1.0
    relation * relation_B = Create_Relation(md,0,0);

    // Radix Sort to r1.0
    relation_B = Radix_Sort(relation_B);

    intervening * interv_final = interveningInit();
    Join_v2(interv_final, relation_A, relation_B, 3, 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].position, 2);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[0], 19193);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].payload[0], 17);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].payload[0], 1525);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].payload[0], 5203);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].payload[0], 7383);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].payload[0], 7833);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].payload[0], 10479);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].payload[0], 11780);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].payload[0], 13434);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].payload[1], 0);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].key, 1);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].payload[0], 14148);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].payload[1], 0);
}

// Join_v2 testing for this particular query: 3 0 1|0.2=1.0&0.1=2.0&0.2<2
void Join_v2_AfterUpdating(void){
		uint64_t num_rows;
    statistics * stats;
    metadata *md = Read_Init_Binary("workloads/small/small.init","workloads/small/",&num_rows,stats);
    // 3 0 1|0.2=1.0&0.1=2.0&0.2<2|1.2 0.1

    // r3.2
    relation * relation_A = Create_Relation(md,3,2);

    // Radix sort to r3.2
    relation_A = Radix_Sort(relation_A);

    // r3.2 < 2
    relation_A = Filter(relation_A, 2, '<');

    // Print_Relation_2(relation_A);

    // r1.0
    relation * relation_B = Create_Relation(md,0,0);

    // Radix Sort to r1.0
    relation_B = Radix_Sort(relation_B);

    intervening * interv_final = interveningInit();
    Join_v2(interv_final, relation_A, relation_B, 3, 0);

    Update_Relation_Keys(md,3,1,interv_final->final_rel,0);

    interv_final->final_rel = Radix_Sort(interv_final->final_rel);

    relation * relation_C = Create_Relation(md,1,0);

    relation_C = Radix_Sort(relation_C);

    Join_v2(interv_final, interv_final->final_rel, relation_C, 3, 1);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].position, 3);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].key, 3706);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[0], 5203);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[0].payload[2], 1216);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].key, 4735);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].payload[0], 7383);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[1].payload[2], 1538);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].key, 4959);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].payload[0], 17);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[2].payload[2], 1616);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].key, 5315);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].payload[0], 7833);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[3].payload[2], 1728);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].key, 5798);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].payload[0], 1525);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[4].payload[2], 1886);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].key, 6397);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].payload[0], 13434);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[5].payload[2], 2088);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].key, 9955);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].payload[0], 14148);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[6].payload[2], 3258);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].key, 11063);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].payload[0], 11780);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[7].payload[2], 3645);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].key, 11240);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].payload[0], 10479);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[8].payload[2], 3696);

    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].key, 11326);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].payload[0], 19193);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].payload[1], 0);
    CU_ASSERT_EQUAL(interv_final->final_rel->tuples[9].payload[2], 3726);
}

// // Checksum testing for this particular query: 3 0 1|0.2=1.0&0.1=2.0&0.2<2|1.2 0.1
// void CheckSum_Simple(void){
// 		uint64_t num_rows;
//     statistics *stats;
//     metadata *md = Read_Init_Binary("workloads/small/small.init","workloads/small/", &num_rows, stats);
//     // 3 0 1|0.2=1.0&0.1=2.0&0.2<2|1.2 0.1
//
//     // r3.2
//     relation * relation_A = Create_Relation(md,3,2);
//
//     // Radix sort to r3.2
//     relation_A = Radix_Sort(relation_A);
//
//     // r3.2 < 2
//     relation_A = Filter(relation_A, 2, '<');
//
//     // Print_Relation_2(relation_A);
//
//     // r1.0
//     relation * relation_B = Create_Relation(md,0,0);
//
//     // Radix Sort to r1.0
//     relation_B = Radix_Sort(relation_B);
//
//     intervening * interv_final = interveningInit();
//     Join_v2(interv_final, relation_A, relation_B, 3, 0);
//
//     Update_Relation_Keys(md,3,1,interv_final->final_rel,0);
//
//
//     interv_final -> final_rel = Radix_Sort(interv_final->final_rel);
//
//     relation * relation_C = Create_Relation(md,1,0);
//
//     relation_C = Radix_Sort(relation_C);
//
//     Join_v2(interv_final, interv_final->final_rel, relation_C, 3, 1);
//
//     printf("\n");
//     CU_ASSERT_EQUAL(CheckSum(md,0,2,interv_final->final_rel,1), 5820);
//     printf("\n");
//     CU_ASSERT_EQUAL(CheckSum(md,3,1,interv_final->final_rel,0), 74494);
//     printf("\n");
// }

int main(int argc, char const *argv[])
{
	CU_pSuite pSuite = NULL;

	// Initialize unit test registry
	if (CU_initialize_registry() != CUE_SUCCESS){
		return CU_get_error();
	}

	// Add test suite to registry
	pSuite = CU_add_suite("sorting suite", &TestInit, &TestCleanup);
	if (pSuite == NULL){
		CU_cleanup_registry();
		return CU_get_error();
	}

	// Add unit tests to the suite
	if ((CU_ADD_TEST(pSuite, Join_v2_Simple)==NULL) ||
        (CU_ADD_TEST(pSuite, Join_v2_AfterUpdating)==NULL) ||
        // (CU_ADD_TEST(pSuite, CheckSum_Simple)==NULL) ||
        (CU_ADD_TEST(pSuite, Scan_Simple)==NULL))
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
