obj-m += modpcb.o  testmodule.o

all:
	gcc -g -Wall -o csort csort.c -lrt
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules

clean:
	rm -fr *~   csort *.o *.ko *.mod.c *.order
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
