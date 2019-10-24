OBJS = build/main.o \
		build/getColumn.o \
		build/initArray.o \
		build/histogram.o \
		build/psum.o \
		build/reorderedColumn.o \
		build/quicksort.o \
		build/cleanRelation.o \
		build/splitBucket.o



CC = gcc
FLAGS = -Wall -Wextra -g -c


TARGET = smj

all: $(TARGET)

clean:
	$(RM) -r $(TARGET) build/*


build/main.o: src/main.c
	$(CC) $(FLAGS) $< -o $@

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


$(TARGET) : $(OBJS)
		$(CC) $(CFLAGS) $^ -o $@

rebuild: clean all
