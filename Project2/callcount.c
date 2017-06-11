/*
 ============================================================================
 Name        : callcount.c
 Author      : Gulsum Gudukbay
 Version     :
 Date		 : 25 October 2016
 ============================================================================
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

struct mapper_thread_args{
	int r, j;
	FILE* inFile;
};
struct reducer_thread_args{
	int n, i;
	unsigned int sID, eID, sYear, eYear;
};
struct call_record{
	unsigned int callerID, calledID, year;
	struct call_record *next;
};
struct count_record{
	unsigned int callerID;
	int count;
	struct count_record *next;
};
struct merger_thread_args{
	int r;
	char* outputFileName;
};

void writeOutput(char* , struct count_record* );
unsigned int** counter(struct call_record*, int);
struct call_record* deleteDuplicates(struct call_record*);
void swap(struct call_record*,struct call_record*);
struct call_record* bubbleSort(struct call_record*);
void* mapper_function(void*);
void* merger_function(void*);
void* reducer_function(void*);
void printLL(struct call_record* );
void freeLL(struct call_record* );
void printLL2(struct count_record* );
void freeLL2(struct count_record* );
int doesExist(unsigned int**, int, unsigned int);
unsigned int countOccurences(unsigned int, struct call_record*);


unsigned int countOccurences(unsigned int a, struct call_record* root){
	struct call_record* cur = root;
	unsigned int count = 0;
	while(cur!=NULL){
		if(cur->callerID == a)
			count++;
		cur = cur->next;
	}
//	printf("COUNT IS: %d\n", count);
	return count;
}

int doesExist(unsigned int** arr,int size,  unsigned int callerID){
	int i;
	for(i = 0; i < size; i++)
		if(arr[i][0] == callerID)
			return 1;
	return 0;
}

unsigned int** counter(struct call_record* root, int nodecount ){
	unsigned int **arr = (unsigned int**)malloc((nodecount+1) * sizeof(unsigned int*));
	int i, index,j;
	for(i = 0; i < nodecount+1; i++)
		arr[i] = (unsigned int*)malloc(sizeof(unsigned int) * 2); //2 cols
	for(i = 0; i < nodecount+1; i++){
		arr[i][1] = 0; // initialize all counts to 0
		arr[i][0] = 0;
	}

	struct call_record* cur = root;
	index = 0;

	while(cur != NULL){
		if(!doesExist(arr, nodecount, cur->callerID)){
			arr[index][0] = cur->callerID;
			index++;
		}
		cur = cur->next;
	}

	//done traversing terminate arr
	arr[index][0] = 999999999; //this number is not in the range!
	arr[index][1] = 999999999; //this number is not in the range!

	j = 0;
	while(arr[j][0] != 999999999){
		arr[j][1] = countOccurences(arr[j][0], root);
		j++;
	}
	return arr;

}

struct call_record* deleteDuplicates(struct call_record* root){

	struct call_record* cur = root;
	struct call_record* nextOfDeleted;

	if(root == NULL)
		return NULL;
	else{
		while(cur->next != NULL){
			if(cur->callerID == cur->next->callerID && cur->calledID == cur->next->calledID){
				nextOfDeleted = cur->next->next;
				free(cur->next);
				cur->next = nextOfDeleted;
			}
			else
				cur = cur->next;
		}
		return root;
	}
}

void swap(struct call_record* a,struct call_record* b){
	unsigned int temp;
	temp = a->callerID;
	a->callerID = b->callerID;
	b->callerID = temp;

	temp = a->calledID;
	a->calledID = b->calledID;
	b->calledID = temp;

	temp = a->year;
	a->year = b->year;
	b->year = temp;
}

//bubbleSort to sort the given linkedlist
struct call_record* bubbleSort(struct call_record* root){

	int isSwapped = 0;
	struct call_record* cur;
	struct call_record* prevptr = NULL;

	if(root == NULL)
		return NULL;
	else{
		do{
			isSwapped = 0;
			cur = root;
			while(cur->next != prevptr){
				if(cur->callerID > cur->next->callerID){
					swap(cur, cur->next);
					isSwapped = 1; //for efficiency
				}
				cur = cur->next;
			}
		}
		while(isSwapped);
		return root;
	}
}


int main(int argc, char *argv[]){
	double total_time;
	clock_t start, end;
	start = clock();
	int index,j,i;
	int n = atoi(argv[1]);
	int r = atoi(argv[2]);
	int *irets = malloc(sizeof(int)*n); //thread_create method returned ints
	int *irets2 = malloc(sizeof(int)*r); //thread_create method returned ints
	int iret3;
	FILE ** inFiles = malloc(sizeof(FILE*)*n);
	pthread_t *mapper_threads = malloc(sizeof(pthread_t)*n);
	pthread_t *reducer_threads = malloc(sizeof(pthread_t)*r);
	pthread_t merger_thread;

	struct mapper_thread_args *mta = malloc(sizeof(struct mapper_thread_args));
	struct reducer_thread_args *rta = malloc(sizeof(struct reducer_thread_args));
	struct merger_thread_args *mergeta = malloc(sizeof(struct merger_thread_args));

	for(index = 0; index < n; index++){
		inFiles[index] = fopen(argv[index+3], "r"); //create file pointers to input files
		printf("opened file %s\n", argv[index+3]);
	}
	char* outName = argv[3+n];
	unsigned int sID, eID, sYear, eYear;

	sscanf(argv[4+n], "%u", &sYear);
	sscanf(argv[5+n], "%u", &eYear);

	sscanf(argv[6+n], "%u", &sID);
	sscanf(argv[7+n], "%u", &eID);


	printf ("Program started with syear %u, eyear %u, sid %u, eid %u\n", sYear, eYear, sID, eID);

	// Create independent mapper - threads each of which will execute mapper function
	j = 0;
	while(j < n){
		mta->inFile = inFiles[j];
		mta->r = r;
		mta->j = j;
		irets[j] = pthread_create( &mapper_threads[j], NULL, mapper_function, (void*) mta);
		j++;
	}

	// Wait till mapper - threads are complete before main continues.
	for(index = 0; index < n; index++)
		pthread_join( mapper_threads[index], NULL);

	//check if the mapper - threads returned safely
	for(index = 0; index < n; index++)
		printf("Mapper threads return: %d\n",irets[index]);

	/**************************************************************************************************/
	i = 0;
	// Create independent reducer - threads each of which will execute reduce function
	while(i < r){
		rta->n = n;
		rta->i = i;
		rta->sYear = sYear;
		rta->eYear = eYear;
		rta->sID = sID;
		rta->eID = eID;
		irets2[i] = pthread_create( &reducer_threads[i], NULL, reducer_function, (void*) rta);
		i++;
	}

	// Wait till reducer - threads are complete before main continues.
	for(index = 0; index < r; index++)
		pthread_join( reducer_threads[index], NULL);

	//check if the reducer - threads returned safely
	for(index = 0; index < r; index++)
		printf("Reducer threads return: %d\n",irets2[index]);


	/**************************************************************************************************/
	// Create independent merger - thread which will execute merger function
	mergeta->r = r;
	mergeta->outputFileName = outName;
	iret3 = pthread_create(&merger_thread, NULL, merger_function, (void*) mergeta);

	// Wait till merger - thread is complete before main continues.
	pthread_join( merger_thread, NULL);

	//check if the merger - thread returned safely
	printf("Merger thread return: %d\n", iret3);

	free(mergeta);
	free(rta);
	free(mta);
	free(reducer_threads);
	free(mapper_threads);
	for(index = 0; index < n; index++){
		fclose(inFiles[index]);
	}
	free(inFiles);
	free(irets2);
	free(irets);


	//DELETE ALL FILES LEFT
	char* temp = "temp";
	char* curFN = malloc(strlen(temp)+5);
	char* indices = malloc(5);

	//int i, j;
	FILE* curFILE = NULL;
	for(i = 0; i < r; i++){
		for(j = 0; j < n; j++){
			strcpy(curFN, temp);
			sprintf(indices, "%d-%d", j, i); //convert j=index to str
			strcat(curFN, indices);
			if((curFILE = fopen(curFN, "r")) !=NULL){
				fclose(curFILE);
				remove(curFN);
			}
		}
	}
			


	free(indices);
	free(curFN);

	end = clock();
	//time count stops
	total_time = ((double) (end - start)) / CLOCKS_PER_SEC;
	//calculate total time
	printf("\nTime taken: %f ", total_time);

	exit(0);
	return 0;
}

void *reducer_function( void *ptr ){
	struct reducer_thread_args *args;
	args = (struct reducer_thread_args *) ptr;
	int n = args->n;

	int j, index, k;
	int i = args->i;
	unsigned int sYear = args->sYear;
	unsigned int eYear = args->eYear;
	unsigned int sID = args->sID;
	unsigned int eID = args->eID;
	char *temp = "temp";
	char *out = "outt";
	char* indices = malloc(5);
	char *curFN = malloc(strlen(temp)+5);
	char *curFN2 = malloc(strlen(out)+5);
	char **fileNames = malloc((strlen(temp)+5)*n);
	char* ii = malloc(4);
	sprintf(ii, "%d", i);
	char* fnn = malloc(9);
	strcpy(fnn, out);
	strcat(fnn, ii);
	int nodeCount;
	

	index = 0;
	while(index < n){
FILE* currentInputFile;
		strcpy(curFN, temp);
		sprintf(indices, "%d-%d", index, i); //convert j=index to str
		strcat(curFN, indices);
		fileNames[index] = curFN;

		nodeCount = 0;
		struct call_record *head = NULL;
		struct call_record *insertedNode = NULL;
		currentInputFile = fopen(curFN, "r");

		char *word = malloc(24);
		while(currentInputFile != NULL && fgets(word, 24, currentInputFile) != NULL){

			insertedNode = (struct call_record*)malloc(sizeof(struct call_record));
			insertedNode->next = NULL;
			char *caller_ID = malloc(9);
			char *called_ID = malloc(9);
			char *year = malloc(5);

			//if(strlen(word)>10){
			if(strcmp(word, "\n") != 0){
//printf("WORD: %s", word);
				for(j = 0; j < 8; j++)
					caller_ID[j] = word[j];
				caller_ID[8] = '\0';
				for(j = 0; j < 8; j++)
					called_ID[j] = word[j+9];
				called_ID[8] = '\0';
				for(j = 0; j < 4; j++)
					year[j] = word[j+18];
				year[4] = '\0';

				sscanf(caller_ID, "%u", &insertedNode->callerID);
				sscanf(called_ID, "%u", &insertedNode->calledID);
				sscanf(year, "%u", &insertedNode->year);

				if((insertedNode->callerID <= eID && insertedNode->callerID >= sID) && (insertedNode->year <= eYear && insertedNode->year >= sYear)){
//printf("INSERTED NODE: %u %u %u\n", insertedNode->callerID, insertedNode->calledID, insertedNode->year);
					//add to beginning of linkedlist
					insertedNode->next = head;
					head = insertedNode;
					nodeCount++;
				}
				else
					free(insertedNode);
			}
			free(year);
			free(called_ID);
			free(caller_ID);
		}
		//if(head != NULL)
		//	printLL(head);
		head = bubbleSort(head);
		head = deleteDuplicates(head);
		//if(head != NULL)
		//	printLL(head);

		//writeoutputs!!!
		if(head != NULL){
			unsigned int **arr = counter(head, nodeCount);
			k = 0;
			FILE* f2 = fopen(fnn, "w+");
			//printf("WRITE OUTPUTS FNN %s\n", fnn);
			while(arr[k][0]!= 999999999){
				fprintf(f2, "%u\t%u\n", arr[k][0], arr[k][1]);
				k++;
			}
			fclose(f2);

			for(k = 0; k < nodeCount+1; k++)
				free(arr[k]);
			free(arr);
		}

		remove(curFN);
		freeLL(head);
		free(word);
		if(currentInputFile != NULL)
		fclose(currentInputFile);
		//remove(curFN);
		index++;
	}
	free(ii);
	free(fnn);
	free(curFN2);
	free(curFN);
	free(indices);
	free(fileNames);
	return NULL;
}


void *mapper_function( void *ptr ){
	struct mapper_thread_args *args;
	char *temp = "temp";
	args = (struct mapper_thread_args *) ptr;
	int r = args->r;
	int i, index;
	int j = args->j;
	FILE *currentInputFile = args->inFile;
	FILE **temp_files = malloc(sizeof(FILE*)*r);
	char* indices = malloc(5);
	char *ii = malloc(2);
	char *filename = malloc(strlen(temp)+5);
	char *word = malloc(24);
	char *word2 = malloc(23);
	char *caller_id = malloc(9);
	unsigned int callerID;

	int *isempty = malloc(sizeof(int)*r); //is file empty

	for(index = 0; index < r; index++){
		isempty[index] = 1;
	}

	//create r temp files
	for(index = 0; index < r; index++){
		strcpy(filename, temp);
		sprintf(indices, "%d-%d", j,index); //convert j to str
		strcat(filename, indices);
		temp_files[index] = fopen(filename,"a");
	}

	//read from files and do the mapping
	while (fgets(word, 24, currentInputFile) != NULL) {
		if(strcmp(word, "\n")!=0){

			for(index = 0; index < 8; index++)
				caller_id[index] = word[index]; //get the caller id
			caller_id[8] = '\0';//dont forget the terminator
			sscanf(caller_id, "%u", &callerID);
			i = callerID % r;

			//remove \n from word (if there exists one)
			for(index = 0; index < 22; index++)
				word2[index] = word[index];
			word2[22] = '\0';

			//if(isempty[i] == 1){ //if file is empty
			//	fprintf(temp_files[i], "%s" , word2);
			//	isempty[i] = 0; //not empty anymore
			//}
			//else{
				fprintf(temp_files[i], "%s\n", word2);
			//}
		}
	}
	for(index = 0; index < r; index++)
		fclose(temp_files[index]);

	free(isempty);
	free(caller_id);
	free(word2);
	free(word);

	free(filename);
	free(ii);
	free(indices);
	free(temp_files);
	return NULL;
}


void* merger_function(void* ptr){
	char *out = "outt";
	char* indices = malloc(5);
	struct merger_thread_args *args;
	args = (struct merger_thread_args *) ptr;
	int r = args->r;
	char* outputFileName = args->outputFileName;
	int i = 0;
	int j = 0;
	int k = 0;
	int bool;
	int wordsize;
	int filecount = 0;
	unsigned int currMinimum, indexFileMin;
	int* doneFiles;
	struct count_record *finalLL, *finalLLCur, *temp;
	struct count_record **roots, **tails, **rootsSpare;

	doneFiles = (int*)malloc(r*sizeof(int));
	roots = (struct count_record**)malloc(r*sizeof(struct count_record*));
	rootsSpare = roots;
	tails = (struct count_record**)malloc(r*sizeof(struct count_record*));
	for(k = 0; k < r; k++){
		roots[k] = (struct count_record*)malloc(sizeof(struct count_record));
		roots[k]->callerID = 999999999;
		roots[k]->count = 0;
		roots[k]->next = NULL;
		tails[k] = roots[k];
		rootsSpare[k] = roots[k];
		doneFiles[k] = 0;
	}

	finalLL = (struct count_record*)malloc(sizeof(struct count_record));
	finalLLCur = finalLL;
	finalLLCur->callerID = 999999999;
	finalLLCur->count = 0;
	finalLLCur->next = NULL;

	bool = 1;////////////////////////////////////

	//populate linkedlists from files
	for(i = 0; i < r; i++){
		FILE* fptr;
		char *curFN2 = malloc(strlen(out)+5);

		strcpy(curFN2, out);
		sprintf(indices, "%d",i); //convert j=index to str
		strcat(curFN2, indices);
		fptr = fopen(curFN2, "r");
	//	if(fptr != NULL)
		//	printf("\nINDICES %d %s\n", i, curFN2);

		if(fptr != NULL){
			char *word = malloc(15);
			while(fgets(word, 15, fptr) != NULL && (wordsize = (int)strlen(word))>5){
				if(strcmp(word, "\n") != 0){
					char *count = malloc(wordsize-9);
					char* caller_ID = malloc(9);

					for(j = 0; j < 8; j++)
						caller_ID[j] = word[j];
					caller_ID[8] = '\0';
					for(j = 9; j < wordsize-1; j++)
						count[j-9] = word[j];
					count[j-9] = '\0';

					sscanf(caller_ID, "%u", &tails[filecount]->callerID);
					sscanf(count, "%d", &tails[filecount]->count);

					//printf("CALLER ID: %u, COUNT: %d\n", tails[filecount]->callerID, tails[filecount]->count);

					tails[filecount]->next = (struct count_record*)malloc(sizeof(struct count_record));
					tails[filecount] = tails[filecount]->next;
					tails[filecount]->callerID = 999999999; // propagate terminator
					tails[filecount]->next = NULL;

					free(caller_ID);
					free(count);

				}
			}
			filecount++;
			free(word);
			fclose(fptr);
			remove(curFN2);
		}

		free(curFN2);
	}
	bool = 0;
	while(bool == 0){
		currMinimum = 999999999;
		indexFileMin = -1;
		int curcount;

		for(i = 0; i < filecount; i++){
			if(roots[i] != NULL && roots[i]->callerID != 999999999  && roots[i]->callerID < currMinimum ){
				currMinimum = roots[i]->callerID;
				curcount = roots[i]->count;
				indexFileMin = i;
			}
		}
		if(indexFileMin == -1)
			bool = 1; //DONE
		else{
			finalLLCur->callerID = currMinimum;
			finalLLCur->count = curcount;
			finalLLCur->next = (struct count_record*)malloc(sizeof(struct count_record));
			finalLLCur = finalLLCur->next;
			finalLLCur->callerID = 999999999; // propagate terminator
			finalLLCur->next = NULL;
			temp = roots[indexFileMin];
			roots[indexFileMin] = roots[indexFileMin]->next; //propagate min head
			free(temp);
		}

	}
//	printLL2(finalLL);
	writeOutput(outputFileName, finalLL);

	freeLL2(finalLL);
	for(k = 0; k < r; k++)
		freeLL2(rootsSpare[k]);
	free(tails);
	free(rootsSpare);
	free(doneFiles);
	free(indices);
	return NULL;
}


//prints the contents of a linkedlist
void printLL(struct call_record* head){
	if(head!=NULL){
		printf("\n%s", "Contents of linkedlist:\n");
		struct call_record *cur = head;
		while (cur != NULL){
			printf("%u\t", cur->callerID);
			printf("%u\t", cur->calledID);
			printf("%u\n", cur->year);
			cur = cur->next;
		}
	}
	else
		printf("\n%s", "NO LL\n");
}

//deallocation of the allocated dynamic memory
void freeLL(struct call_record* head){
	struct call_record* previousNode = head;
	struct call_record* currNode = head;

	while(currNode != NULL){
		currNode = previousNode->next;
		free(previousNode);
		previousNode = currNode;
	}
}


//prints the contents of a linkedlist
void printLL2(struct count_record* head){
	if(head!=NULL){
		printf("\n%s", "Contents of count linkedlist:\n");
		struct count_record *cur = head;
		while (cur != NULL && cur->callerID != 999999999){
			printf("%u\t%u\n", cur->callerID, cur->count);
			cur = cur->next;
		}
	}
	else
		printf("\n%s", "NO LL\n");
}

//deallocation of the allocated dynamic memory
void freeLL2(struct count_record* head){
	struct count_record* previousNode = head;
	struct count_record* currNode = head;

	while(currNode != NULL){
		currNode = previousNode->next;
		free(previousNode);
		previousNode = currNode;
	}
}

void writeOutput(char* filename, struct count_record* rootOutput){
	if(rootOutput != NULL){
		FILE *out;
		struct count_record* cur = rootOutput;
		out = fopen(filename, "w+");

		while(cur != NULL && cur->callerID != 999999999){
			char concat[256];
			sprintf(concat, "%u\t%d\n", cur->callerID, cur->count);
			fprintf(out, "%s", concat);

			cur = cur->next;
		}

		fclose(out);
	}
}


