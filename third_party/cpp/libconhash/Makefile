
# output path
PREFIX=bin

# debug stuff 
ifeq ($(CFLAG), DEBUG)
CFLAGS += -g
endif

AR  = ar -cqs
# itermidiate objects
OBJ = $(addprefix $(PREFIX)/, \
	  md5.o \
	  util_rbtree.o  \
	  conhash_inter.o \
	  conhash_util.o  \
	  conhash.o \
	  )

SAMPLE_OBJS = $(addprefix $(PREFIX)/, \
		sample.o \
		)
		
# include file path
INC = -I.

TARGETS = $(PREFIX)/libconhash.a 
#$(PREFIX)/sample
 
all : $(TARGETS)

# build libconhash as a static lib 
$(PREFIX)/libconhash.a : $(OBJ) 
	$(AR) $@ $(OBJ)
	
# build sample
$(PREFIX)/sample : $(SAMPLE_OBJS)
	gcc -O -o $@ $(SAMPLE_OBJS) -L. -L./bin -lconhash

	
$(PREFIX)/%.o : %.c
	gcc $(INC) $(CFLAGS) -c $< -o $@	
