OBJS = build/main.o \
		build/getColumn.o \
		build/initArray.o \
		build/histogram.o \
		build/psum.o \
		build/reorderedColumn.o \
		build/quicksort.o \
		build/cleanRelation.o \
		build/splitBucket.o \
		build/result.o \
		build/processRelation.o \
		build/sortMergeJoin.o \
		build/intervening.o \
		build/work.o \
		build/executeQuery.o \
		build/statistics.o \
		build/bestTree.o \
		build/jobScheduler.o

TEST_OBJS = $(filter-out build/main.o, $(OBJS))

CC = gcc
FLAGS = -Wall -Wextra -g -c
CUNIT_FLAG = -lcunit


TARGET = smj
TARGET_TEST = unit_test

all: $(TARGET) $(TARGET_TEST)

clean:
	$(RM) -r $(TARGET) $(TARGET_TEST) build/*

build/main.o: src/main.c
	$(CC) $(FLAGS) $< -o $@

build/unit_testing.o: unit_testing.c
	$(CC) $(FLAGS) $< -o $@ $(CUNIT_FLAG)

build/getColumn.o: src/getColumn.c
	$(CC) $(FLAGS) $< -o $@

build/initArray.o: src/initArray.c
	$(CC) $(FLAGS) $< -o $@

build/histogram.o: src/histogram.c
	$(CC) $(FLAGS) $< -o $@

build/psum.o: src/psum.c
	$(CC) $(FLAGS) $< -o $@

build/reorderedColumn.o: src/reorderedColumn.c
	$(CC) $(FLAGS) $< -o $@

build/quicksort.o: src/quicksort.c
	$(CC) $(FLAGS) $< -o $@

build/cleanRelation.o: src/cleanRelation.c
	$(CC) $(FLAGS) $< -o $@

build/splitBucket.o: src/splitBucket.c
	$(CC) $(FLAGS) $< -o $@

build/result.o: src/result.c
	$(CC) $(FLAGS) $< -o $@

build/processRelation.o: src/processRelation.c
	$(CC) $(FLAGS) $< -o $@

build/sortMergeJoin.o: src/sortMergeJoin.c
	$(CC) $(FLAGS) $< -o $@

build/intervening.o: src/intervening.c
	$(CC) $(FLAGS) $< -o $@

build/executeQuery.o: src/executeQuery.c
	$(CC) $(FLAGS) $< -o $@

build/work.o: src/work.c
	$(CC) $(FLAGS) $< -o $@

build/statistics.o: src/statistics.c
	$(CC) $(FLAGS) $< -o $@

build/bestTree.o: src/bestTree.c
	$(CC) $(FLAGS) $< -o $@

build/jobScheduler.o: src/jobScheduler.c
	$(CC) $(FLAGS) $< -o $@

$(TARGET) : $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ -lm -lpthread

$(TARGET_TEST) : unit_testing.c $(TEST_OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(CUNIT_FLAG) -lm -lpthread

rebuild: clean all
