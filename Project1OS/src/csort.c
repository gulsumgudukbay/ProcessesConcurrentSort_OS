#define MQNAME "/mqname"
#define MAXINT 2000000000
#include <stdlib.h>
#include <mqueue.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

struct Node {
	int data;
	struct Node *next;
};
struct message{
	int data;
};
char word[9];

struct Node* readAndInsertToLL(char*, long, int);
void swap(struct Node*, struct Node*);
struct Node* selectionSort(struct Node*);
void writeOutput(char*, struct Node*);
void printLL(struct Node*);
void freeLL(struct Node*);
int binaryToInteger(char *);
int mqProcessor( mqd_t, char*, int, int, int, int, int);
void merger(char*, mqd_t[], int);

void merger(char* outputFileName, mqd_t mqs[], int n){ //daha bitmedi
	FILE *fp;
	struct mq_attr mq_attr;
	int x; //to hold the received int
	int bufferLength; //length of the received buffer
	int queueStarts[n]; //numbers at the start of queues
	int currMinimum; //current minimum value that's gonna be written to the file
	int indexQueueMin;//index of the queue that has currMinimum
	int numBytes; //no of bytes that are received
	int doneqNo; //Number of queues that have been processed
	int index; //index of for loop
	doneqNo = 0;

	fp = fopen(outputFileName, "w");
	mq_getattr(mqs[0], &mq_attr);
	bufferLength = mq_attr.mq_msgsize;
	//x = (int)malloc(bufferLength);

	//reading the first integers from each queue
	for(index = 0; index < n; ++index){
		numBytes = mq_receive(mqs[index], &x, bufferLength, NULL);
		if(numBytes == -1){
			perror("mq_receive failed \n");
			exit(1);
		}
		if(x == -99){
			queueStarts[index] = x;
			doneqNo++;
		}
		else
			queueStarts[index] = x;

	}

	while(doneqNo < n){
		indexQueueMin = -99;
		currMinimum = MAXINT;
		for(index = 0; index < n; ++index){
			if(queueStarts[index] != -99){
				if(queueStarts[index] < currMinimum){
					currMinimum = queueStarts[index];
					indexQueueMin = index;
				}
			}
		}
		printf("writing...%d\n", currMinimum);
		fprintf(fp, "%d\n", currMinimum);//write to file the current min
		//read from queue
		numBytes = mq_receive(mqs[indexQueueMin], &x, bufferLength, NULL);
		if(n == -1){
			perror("mq_receive failed\n");
			exit(1);
		}
		if(x == -99){
			queueStarts[index] = x;
			doneqNo++;
		}
		else
			queueStarts[index] = x;
	}

	fprintf(fp, "%d\n", 99999);
	fclose(fp);
}

//reads, sorts, sends all the numbers in a ll to mq
int mqProcessor(mqd_t mq, char* filename, int filesize, int bytesToBeReadByEachProcess, int index, int n, int t) {
	int y, terminator, mqsendvalue;
	terminator = -99;
	y = bytesToBeReadByEachProcess / 9 * 9;
	struct Node* root;
	struct Node* cur;
	//struct message msg;
	//msg = (struct message)malloc(sizeof(struct message));
	if (t == n - 1)
		root = readAndInsertToLL(filename, index, (y + (filesize - n * y)));
	else
		root = readAndInsertToLL(filename, index, y);

	cur = root;
	//BURAYA ERISEMIYOR
	while (cur != NULL) {
		//msg.data = cur->data;
		printf("mq sended value: %d", cur->data);

		mqsendvalue = mq_send(mq, (char *) (&cur->data), sizeof(int), 1);
		if(mqsendvalue == -1){
			perror("mq_send failed\n");
			exit(1);
		}
		printf("mq send value: %d", mqsendvalue);
		cur = cur->next;
	}
	//msg.data = terminator;
	printf("mq send value last: %d", mqsendvalue);
	mqsendvalue = mq_send(mq, (char *) (&terminator), sizeof(struct message), 1);
	if(mqsendvalue == -1){
		perror("mq_send terminator failed\n");
		exit(1);
	}

	freeLL(root);
	return -1;
}

int main(int argc, char *argv[]) {
	const int n = atoi(argv[1]);
	int i, j, k;
	pid_t pids[n];
	pid_t pid, mergerpid;
	mqd_t mqs[n];
	const char mq_names[n][10];
	struct mq_attr mq_attr[n];
	int filesize, bytesToBeReadByEachProcess;
	char* filename;
	filename = argv[2];
	char* outputFileName = argv[3];
	FILE* fp = fopen(filename, "r");
	fseek(fp, 0L, SEEK_END);
	filesize = ftell(fp);
	rewind(fp);
	fclose(fp);
	bytesToBeReadByEachProcess = filesize / n;

	//create msg queues
	for (j = 0; j < n; j++) {
		sprintf(mq_names[j], "%s%d", MQNAME, j);
		mqs[j] = mq_open(mq_names[j], O_RDWR | O_CREAT, 0666, NULL);

		if (mqs[j] == -1) {
			perror("cannot create msg queue\n");
			exit(1);
		}
		printf("mq created, mq id = %d\n", (int) mqs[j]);
		mq_getattr(mqs[j], &mq_attr[j]);
		printf("mq maximum msgsize = %d\n", (int) mq_attr[j].mq_msgsize);
	}

	for (k = 0; k < n; ++k)
		sprintf(mq_names[k], "%d", k);

	int t, y;
	t = 0;
	y = bytesToBeReadByEachProcess / 9 * 9;
	printf("pos: %d\n", y);
	/*for (k = 0; k <= filesize && t < n; k = y * t) {
		mqProcessor(mqs[t], filename, filesize, bytesToBeReadByEachProcess, k, n, t);
		t++;
	}*/

	//create merger process
	pid = fork();
	if(pid == 0){
		merger(outputFileName, mqs,n);
		exit(0);
	}
	mergerpid = pid;

	//create sorter processes
	for (k = 0; k <= filesize && t < n; k = y * t) {
		pid = fork();
		if ((pid = fork()) < 0) { //child cannot be created error!
			perror("fork");
			abort();
		} else if (pid == 0) { //in the child process
			mqProcessor(mqs[t], filename, filesize, bytesToBeReadByEachProcess, k, n, t);
			printf("Process id done: %d", pid);

			exit(0);
		}
		pids[t] = pid;
		t++;
	}

	//wait for merger and sorter processes to end
	for (j = 0; j < n; ++j) {
		waitpid(pids[j], NULL, 0);
	}
	waitpid(mergerpid, NULL, 0);

	//remove message queues
	for (j = 0; j < n; ++j) {
		mq_close(mqs[j]);
		mq_unlink(mq_names[j]);
	}

	return 0;
}

// sort yap
//sayilari mqa gonder
//tum sayilar bitince -99 gonder
//mqlardan -99 almaya baslayinca ana process de merge et ve file a yaz

//main will create n message queues and n worker processes, childs mem is
//populated from parent's mem. message queues are used to exchange info
//after creating the worker processes, the main process will wait for
//sorted integers to arrive from msg queues
//each worker will read ints from a portion of input and sort them via linked
//lists. then sorted ints will be sent via the respective msg queue and merge
//them efficiently (that is when receiving) into a output text file (by
//retrieving the min int form the head of queues each time)
//when a worker has sent all ints, it will terminate and bef termination it can
//send an int to indicate end of stream. Before termination main will remove
//all msg queues.

int binaryToInteger(char *str) {
	int i, result, multiplier;
	result = 0;
	multiplier = 0;

	for (i = 7; i >= 0; i--) {
		result = result + ((str[i] - 48) * (1 << multiplier++));
	}
	return result;
}

//reads from file and inserts the contents to a linkedlist as integers
struct Node* readAndInsertToLL(char* filename, long pos, int bytesToBeReadByEachProcess) {
	FILE* file = fopen(filename, "r");
	int fd = fileno(file);
	struct Node* head = NULL;
	struct Node* insertedNode = NULL;
	int loop, i;
	loop = bytesToBeReadByEachProcess / 9;
	i = 0;
	if (lseek(fd, pos, 0) >= 0) { //go to pos
		while (i < loop && read(fd, word, 9) >= 0) { //read 9 bytes everytime
			insertedNode = (struct Node*) malloc(sizeof(struct Node));
			int receivedData = binaryToInteger(word);
			insertedNode->data = receivedData;
			insertedNode->next = NULL;
			if (head == NULL) {
				head = insertedNode;
			} else {
				insertedNode->next = head;
				head = insertedNode;
			}
			i++;
		}
	}

	fclose(file);
	struct Node* head2;
	printLL(head);
	head2 = selectionSort(head);
	printLL(head2);
	return head2;
}

//utility function for selection sort, swaps the contents of nodes
void swap(struct Node* a, struct Node* b) {
	int temp = a->data;
	a->data = b->data;
	b->data = temp;
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

//selection sort to sort the given linkedlist
struct Node* selectionSort(struct Node* root) {

	struct Node *start = root;
	struct Node *cur;
	struct Node *min;

	while (start->next) {
		min = start;
		cur = start->next;

		while (cur) {
			if (min->data > cur->data)
				min = cur; //find min
			cur = cur->next;
		}
		swap(start, min); //insert min to beginning
		start = start->next;
	}
	return root;
}

//called after output linked list is sorted
void writeOutput(char* filename, struct Node* rootOutput) {
	FILE *out;
	struct Node* cur = rootOutput;
	out = fopen(filename, "w+");

	while (cur != NULL) {
		char concat[8];
		sprintf(concat, "%d", cur->data); //CHECK!
		printf(concat);
		strcat(concat, "\n");

		fprintf(out, concat);
		cur = cur->next;
	}
	fclose(out);
}

void printLL(struct Node* head) {
	if (head != NULL) {
		printf("\n%s", "Contents of linkedlist: \n");

		struct Node *cur = head;
		while (cur != NULL) {
			printf("%d\n", cur->data);
			cur = cur->next;
		}
	} else
		printf("%s", "NO LL");
}
