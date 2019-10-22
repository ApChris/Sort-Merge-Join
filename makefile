OBJS = build/main.o \
		build/getColumn.o \
		build/initArray.o


CC = g++
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


$(TARGET) : $(OBJS)
		$(CC) $(CFLAGS) $^ -o $@

rebuild: clean all