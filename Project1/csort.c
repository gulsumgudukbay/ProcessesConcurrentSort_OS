/*
 * csort.c
 *
 * Created on: Oct 9, 2016
 * Author: Gulsum Gudukbay
 * Student ID: 21401148
 */
#define MQNAME "/mqname"
#define MAXINT 2000000000
#include <stdlib.h>
#include <mqueue.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

struct Node {
	long long int data;
	struct Node *next;
};

int waitpid(pid_t, int*, int);
void printLL(struct Node*);
void freeLL(struct Node*);
void workerP(mqd_t, struct mq_attr, int, int, int, int);
void merger(FILE*, mqd_t[], int);

void quickSort(char*, long long int, long long int);
int partition(char*, long long int, long long int);
void swap(long long int*, long long int*);

void swap(long long int* a, long long int* b){
	long long int temp;
	temp = *a;
	*a = *b;
	*b = temp;
}

int partition(char* buf, long long int first, long long int last){

	long long int pivot, i, j;
	i = first;
	j = last;
	pivot = first;

	while(i < j){
		while((((long long*)buf)[i] <= ((long long*)buf)[pivot]) && (i < last)) i++;
		while(((long long*)buf)[j] > ((long long*)buf)[pivot]) j--;

		if(i < j)
			swap(&((long long*)buf)[i], &((long long*)buf)[j]);
	}
	swap(&((long long*)buf)[pivot], &((long long*)buf)[j]);
	return j;
}

void quickSort(char* buf, long long int first, long long int last){
    long long int part;
    if( first < last ) {
    	part = partition( buf, first, last );
    	quickSort( buf, first, part - 1LL );
    	quickSort( buf, part + 1LL, last );
    }
}

void workerP(mqd_t mq, struct mq_attr attr, int noOfChildPs, int currentProcessNo, int filesize, int fd){
	long long int sentNo;
	long long int noOfLongIntegers = filesize/8LL;
	long long int noOfArrItems;
	long long int offset = (noOfLongIntegers / noOfChildPs) * currentProcessNo * 8LL;
	long long int bufLength;
	int forindex;
	char* buf;

	bufLength = 0LL;

	if(currentProcessNo == noOfChildPs-1) //remainder + the regular work
		bufLength = filesize - offset;
	else
		bufLength = (noOfLongIntegers / noOfChildPs) * 8LL;

	buf = (char*)malloc(bufLength * sizeof(char));

	noOfArrItems = bufLength / 8LL;

	//go to the position in file according to the offset
	lseek(fd, offset, 0);
	read(fd, buf, bufLength);

	quickSort(buf, 0LL, noOfArrItems-1LL);

	struct Node* head = (struct Node*) malloc(sizeof(struct Node));
	struct Node* cur = head;

	for(forindex = 0; forindex < noOfArrItems; forindex++){
		cur->data = ((long long*)buf)[forindex];
		cur->next = (struct Node*) malloc(sizeof(struct Node));
		cur = cur->next;
		cur->data = 0LL; //propagate terminator
	}

	long long int mqsendvalue;
	cur = head;
	while (cur != NULL) {
		mqsendvalue = mq_send(mq, (char *) (&cur->data), 8, 0);
		if(mqsendvalue == -1){
			printf("mq_send failed%lld\n", mqsendvalue);
			exit(1);
		}
		cur = cur->next;
	}

	freeLL(head);
	free(buf);

}

int main(int argc, char *argv[]) {
	int n = atoi(argv[1]);
	int i, j, k;
	pid_t pids[n];
	pid_t pid, mergerpid;
	mqd_t mqs[n];
	const char mq_names[n][10];
	struct mq_attr mq_attr;

	//set attributes of mq
	mq_attr.mq_flags = 0;
	mq_attr.mq_maxmsg = 8;
	mq_attr.mq_msgsize = 8;
	mq_attr.mq_curmsgs = 0;

	struct mq_attr mq_attr_arr[n];
	int filesize;
	char* filename;
	filename = argv[2];
	char* outputFileName = argv[3];

	int fd = open(filename, O_RDONLY, 0);
	FILE* fp = fopen(outputFileName, "w+");
	filesize = lseek(fd, 0, SEEK_END);

	for(j = 0; j < n; j++)
		sprintf(mq_names[j], "%s%d", MQNAME, j);

	for(j = 0; j < n; j++)
		mq_unlink(mq_names[j]);

	//create msg queues
	for (j = 0; j < n; j++) {
		mqs[j] = mq_open(mq_names[j], O_RDWR | O_CREAT, 0666, &mq_attr);

		if (mqs[j] == -1) {
			printf("cannot create msg queue %d\n", j);
			exit(1);
		}
		printf("mq created, mq id = %d\n", (int) mqs[j]);
		mq_getattr(mqs[j], &mq_attr_arr[j]);
		printf("mq maximum msgsize = %d\n", (int) mq_attr.mq_msgsize);
	}

	//create sorter processes
	for (k = 0; k < n; k++) {
		pid = fork();
		if (pid == 0) { //in the child process
			workerP(mqs[k], mq_attr_arr[k], n, k, filesize, fd);
			exit(0);
		}
		pids[k] = pid;
	}

	merger(fp, mqs,n);

	//wait for sorter processes to end
	for (j = 0; j < n; j++) {
		waitpid(pids[j], NULL, 0);
	}

	//close message queues
	for (j = 0; j < n; ++j) {
		mq_close(mqs[j]);
	}
	fclose(fp);
	close(fd);

	return 0;
}

void merger(FILE* fp, mqd_t mqs[], int n){
	int i, j, k, test,bool;
	struct mq_attr mq_attr;
	long long int currMinimum; //current minimum value that's gonna be written to the file
	long long int indexQueueMin;//index of the queue that has currMinimum
	long long int index; //index of for loop
	int *doneQueues; //arr for holding bools for queues indicating their termination status
	struct Node *finalLL, *finalLLCur, *temp; //list
	struct Node **roots, **tails, **rootsYedek; //list root and tail pointer pointers

	//initializations
	doneQueues = (int*)malloc(n*sizeof(int));
	mq_getattr(mqs[0], &mq_attr);


	roots = (struct Node**)malloc(n*sizeof(struct Node*));
	rootsYedek = roots;
	tails = (struct Node**)malloc(n*sizeof(struct Node*));
	for(j = 0; j < n; j++){
		roots[j] = (struct Node*)malloc(sizeof(struct Node));
		tails[j] = roots[j];
		rootsYedek[j] = roots[j];
	}

	for(i = 0; i < n; i++)
		doneQueues[i] = 0; //false assumption

	//initialize final linked list
	finalLL = (struct Node*)malloc(sizeof(struct Node));
	finalLLCur = finalLL;

	bool = 1; //still have unfinished queues
	while(bool == 1){
		currMinimum = 9223372036854775807LL;
		indexQueueMin = -1;

		for(index = 0; index < n; index++){
			if(doneQueues[index] != 1){ // if the queue is not terminated
				mq_receive(mqs[index], (char*) &(tails[index]->data), 8, 0);

				if(tails[index]->data == 0LL)
					doneQueues[index] = 1; //queue is done
				else{//insert to index-th linked list's tail
					tails[index]->next = (struct Node*)malloc(sizeof(struct Node));
					tails[index] = tails[index]->next;
					tails[index]->data = 0LL; //propagate terminator
				}
			}
		}

		//find the minimum from the beginning of queues' linkedlists
		for(index = 0; index < n; index++){
			if(roots[index]->data != 0LL &&  roots[index]->data < currMinimum){
				currMinimum = roots[index]->data; //set current minimum
				indexQueueMin = index;
			}
		}

		if(indexQueueMin == -1 || currMinimum == 0LL){ //done!
			bool = 0;
		}
		else{ //add to final list
			finalLLCur->data = currMinimum;
			finalLLCur->next = (struct Node*)malloc(sizeof(struct Node));
			finalLLCur = finalLLCur->next;
			finalLLCur->data = 0LL;

			//propagate the root for the indexQueueMin-th LL's root
			roots[indexQueueMin] = roots[indexQueueMin]->next;
		}
	}

	printf("***************\n");

	//printLL(finalLL);

	finalLLCur = finalLL; //take the cur pointer to the beginning
	while(finalLLCur && finalLLCur->data){
		printf("writing...%lld\n", finalLLCur->data);
		fprintf(fp, "%lld\n", finalLLCur->data);//write to file the current node
		finalLLCur = finalLLCur->next;
	}

	freeLL(finalLL);
	for(k = 0; k < n; k++)
		freeLL(rootsYedek[k]);
	free(tails);
	free(rootsYedek);
	free(doneQueues);
}



//deallocation of the allocated dynamic memory
void freeLL(struct Node* head) {
	struct Node* previousNode = head;
	struct Node* currNode = head;

	while (currNode != NULL) {
		currNode = previousNode->next;
		free(previousNode);
		previousNode = currNode;
	}
}


void printLL(struct Node* head) {
	if (head != NULL ) {
		printf("\n%s", "Contents of linkedlist: \n");

		struct Node *cur = head;
		while (cur != NULL ) {
			printf("%lld\n", cur->data);
			cur = cur->next;
		}
		printf("---------------------------------\n");

	} else
		printf("%s", "NO LL");
}

