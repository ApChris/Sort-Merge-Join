OBJS = build/main.o \
		build/getQolumn.o


CC = g++
FLAGS = -Wall -Wextra -g -c


TARGET = smj

all: $(TARGET)

clean:
	$(RM) -r $(TARGET) build/*


build/main.o: src/main.c
	$(CC) $(FLAGS) $< -o $@

build/getQolumn.o: src/getQolumn.c
	$(CC) $(FLAGS) $< -o $@

$(TARGET) : $(OBJS)
		$(CC) $(CFLAGS) $^ -o $@
