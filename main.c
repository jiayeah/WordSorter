#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>

#define DEBUG 
#ifdef DEBUG
#define print(fmt,args...) do{ \
    printf("%s:%d      ",__func__,__LINE__);    \
    printf(fmt,##args);    \
}while(0);          
#else
#define print(fmt, args...)
#endif

/* time debug*/
#define TIME_READ 
#define TIME_WRITE 
#define TIME_SORT 
//#define TIME_SORT_SINGLE 
#define TIME_MEGRE  

/* time type*/
#define READ 0
#define WRITE 1
#define SORT 2
#define MEGRE 3 
#define SORT_SINGLE 4 

#define THREAD_NUM  16 

#ifdef USE_SPINLOCK 
#define LOCK pthread_spin_lock(&g_spinlock); 
#define UNLOCK   pthread_spin_unlock(&g_spinlock);
#define INITLOCK pthread_spin_init(&g_spinlock); 
#define DESTROYLOCK pthread_spin_destroy(&g_spinlock); 
#else   
#define LOCK pthread_mutex_lock(&g_mutex); 
#define UNLOCK  pthread_mutex_unlock(&g_mutex);
#define INITLOCK pthread_mutex_init(&g_mutex); 
#define DESTROYLOCK pthread_mutex_destroy(&g_mutex); 
#endif
    
#define MAXLINE  32 
struct word{
    char          value[MAXLINE];
    int           len;
    char    *     vp;
    struct word * next;
    char          patch[8];
}node;

struct merge_node{
    struct word * word_list;
    struct merge_node *next;
};

struct thread_args
{
    struct merge_node mege;
    int begin;
    int end;
    pthread_t tid;
};

struct  merge_node * g_list = NULL;
struct  merge_node * g_tail = NULL;
#ifdef USE_SPINLOCK
pthread_spinlock_t g_spinlock;
#else
pthread_mutex_t g_mutex;
#endif


void usetime(struct timeval *end, struct timeval *begin, int flag){

    float delay;
    char buf[16];
    if( (end->tv_usec -= begin->tv_usec) < 0)
    {
        end->tv_sec--;
        end->tv_usec += 1000000;
    }

    end->tv_sec -= begin->tv_sec;
    delay = end->tv_sec * 1000 + end->tv_usec/1000;
    switch(flag)
    {
        case READ:
            sprintf(buf,"READ:");
            break;
        case WRITE:
            sprintf(buf,"WRITE:"); 
            break; 
        case SORT:
            sprintf(buf,"SORT:");
            break;
        case SORT_SINGLE:
            sprintf(buf,"SORT_SINGLE:");
            break;
        case MEGRE:
            sprintf(buf,"MEGRE:");
            break;
        default:
            sprintf(buf,"default ");

    }
    print("%s usetime = %f ms\n", buf, delay);


}
struct word * readfile2list(char* filename)
{
    print("read fp : %s\n",filename);
#ifdef TIME_READ
    struct timeval begin,end;
    gettimeofday(&begin, NULL);
#endif
    int ret = -1;
    FILE * fp = NULL;
    size_t size = 0;
    struct word * head, *cur, *prev;
    char firstline[MAXLINE];
    int nums = 0;
    int i = 0;

    fp = fopen(filename,"r+");
    if(fp == NULL)
        exit(EXIT_FAILURE);
    if(fgets(firstline, MAXLINE, fp)!= NULL)
    {
        nums = atoi(firstline);
        print("num of words : %d\n",nums);
    }
    size = (nums + 1) * sizeof(struct word);
    //print("sizeof struct word : %lu, total size = %u\n",sizeof(struct word), size);
    head = (struct word*)calloc(1,size);
    head->len = nums;
    prev = head;
    cur =  head + 1;

    while( fgets(cur->value, MAXLINE, fp) != NULL){
        cur->len = strlen(cur->value);
        cur->value[cur->len-1]='\0';
        cur->vp = cur->value;
        //print("word: %s, len:%d, index:%d\n",cur->value, cur->len,i++);
        prev->next = cur;
        prev = cur;
        cur++;
    }
    fclose(fp);
#ifdef TIME_READ
    gettimeofday(&end, NULL);
    usetime(&end, &begin, READ);
#endif
    return head;
}

int writenode2file(char* filename, struct word * word_list)
{
    print("write fp : %s\n",filename);
    assert(filename!=NULL && word_list != NULL);

#ifdef TIME_WRITE
    struct timeval begin,end;
    gettimeofday(&begin, NULL);
#endif
    FILE * fp = NULL;
    int nums = 0;
    struct word * cur;
    int i;
    fp = fopen(filename, "w+");
    if(fp == NULL)
        exit(EXIT_FAILURE);

    nums = word_list->len;
    print("num of words : %d\n",nums);
    cur = word_list->next;
    while(cur != NULL)
    {
        //    print("cprev : %p, cnext : %p, cur:%p\n",cur->prev, cur->next, cur);
        fputs(cur->vp,fp);
        fputs("\n",fp);
        cur = cur->next;
    }
    fclose(fp);

#ifdef TIME_WRITE
    gettimeofday(&end, NULL);
    usetime(&end, &begin, WRITE);
#endif
    return 0;
}

/*swap value pointer of word*/
int strswap(struct word * a, struct word * b)
{
    char * tmp;
    tmp = a->vp;
    a->vp = b->vp;
    b->vp = tmp;
}

/*quick sort of words */
void qwordsort(struct word * strarray, int begin, int end)
{
    if(begin >= end) return;
    struct word * item = &strarray[begin];
    int low = begin, high = end;
    //print("a=%s,  b=%s\n",strarray[begin].vp, strarray[end].vp);
    while( low < high)
    {
        while(low < high && strcmp(item->vp, strarray[high].vp) <= 0) high--;
        while(low < high && strcmp(item->vp, strarray[low].vp) >= 0) low++;
        strswap(&strarray[low],&strarray[high]);
    }
    if(low!=begin)
        strswap(&strarray[low],item);
    qwordsort(strarray,begin, low-1 );
    qwordsort(strarray,low+1, end );

}

/*not mulit-thread sorting words */
int sortwords(struct word * word_list)
{
    assert(word_list != NULL);
    struct timeval begin,end;
    gettimeofday(&begin, NULL);
    int i;
    int nums = word_list->len;
    struct word *cur;
    cur = word_list->next;
    qwordsort(cur, 0, nums-1);
    gettimeofday(&end, NULL);
    usetime(&end, &begin, SORT_SINGLE);
    return 0;
}
struct merge_node *  get_from()
{
    struct merge_node *  tmp = NULL;
    LOCK;

    if(g_list != NULL)
    {
        tmp = g_list; 
        g_list = tmp->next;
        tmp->next = NULL;
    }

    UNLOCK;
    return tmp;
}

struct merge_node *  insert_into(struct merge_node * mege)
{
    LOCK;

    //print("mege=%p, mege.wl=%p\n",mege,mege->word_list);
    if(g_list == NULL)
    {
        g_list = mege;
        g_tail = mege;
    }
    else
    {
       g_tail->next = mege;
       g_tail = mege;
    }

    UNLOCK;
}

struct word * merge(struct word * wd1, struct word * wd2)
{
    struct word * res = NULL, *prev = NULL; 
    if (strcmp(wd1->vp, wd2->vp) <= 0)
    {
        res = prev=wd1;
        wd1=wd1->next;
    }
    else {
        res = prev=wd2;
        wd2=wd2->next;
    }

    while(wd1 != NULL && wd2 !=NULL)
    {
        if (strcmp(wd1->vp, wd2->vp) <= 0)
        {
            prev->next = wd1;
            prev=wd1;
            wd1=wd1->next;
        }
        else
        {
            prev->next = wd2;
            prev=wd2;
            wd2=wd2->next;
        }
    }
    if(wd1 != NULL)
    {
        prev->next = wd1;
    }
    else
    {
        prev->next = wd2;
    }
    return res;
}

void qsort_thread(struct thread_args *args)
{
    struct timeval begin,end;
    struct merge_node * other = NULL, * m = NULL;

#ifdef TIME_SORT_SINGLE
    gettimeofday(&begin, NULL);
#endif

    //print("pid %d, begin:%d, end:%d\n",args->tid, low, high);
    qwordsort(args->mege.word_list, args->begin, args->end);
#ifdef IN_THREAD_MEGRE  
    other = get_from();
    if(other != NULL)
    {
            print("in thread merge\n");
            m = merge(other->word_list,args->mege.word_list);
            other->word_list = m;
            insert_into(other);
    }
    else
#endif
        insert_into(&(args->mege));

#ifdef TIME_SORT_SINGLE
    gettimeofday(&end, NULL);
    usetime(&end, &begin, SORT);
#endif
}


/*mulit-thread sorting words */
int sortwords_multi(struct word * wlist_head)
{
    pthread_t id[THREAD_NUM];
    int nums = wlist_head->len;
    struct thread_args * targs[THREAD_NUM];
    struct thread_args * leftarg;
    void * res;
    int ret = -1;
    int i = 0;
    int period = nums/THREAD_NUM;
    int left = nums % THREAD_NUM;
    struct word * word_list = wlist_head->next;
    struct merge_node * one=NULL,*two=NULL, *m=NULL;
    struct timeval begin,end;

#ifdef TIME_SORT
    gettimeofday(&begin, NULL);
#endif
    for(i = 0; i < THREAD_NUM; i++)
    {
        targs[i] = calloc(1,sizeof(struct thread_args));
        if(targs[i] == NULL)
        {
            printf("calloc fail\n");
        }
        word_list[(i+1)*period - 1].next = NULL;
        targs[i]->begin  =   0; 
        targs[i]->end    =   period-1;
        targs[i]->tid    =   i+1;
        targs[i]->mege.word_list = &word_list[i*period];
        ret = pthread_create(&id[i],NULL,qsort_thread,targs[i]);
        if(ret<0)
        {
            printf("pthread create error %d\n",i);
        }
    }
    if(left!=0)
    {
        //print("pid %d, left=%d, begin:%d, end:%d \n",0,left, i*period, nums-1);
        leftarg = calloc(1,sizeof(struct thread_args));
        if(leftarg == NULL)
        {
            printf("calloc fail\n");
        }
        leftarg->begin = 0;
        leftarg->end = nums - i*period - 1;
        leftarg->mege.word_list = &word_list[i*period];
        qsort_thread(leftarg);
    }

    for(i = 0; i < THREAD_NUM; i++)
    {
        ret = pthread_join(id[i],NULL);
    }

#ifdef TIME_SORT
    gettimeofday(&end, NULL);
    usetime(&end, &begin, SORT);
#endif
#ifdef TIME_MEGRE
    gettimeofday(&begin, NULL);
#endif
   // merge
    while(1){
        if(g_list == NULL)
        {
            break;
        }
        if(one == NULL)
        {
            one = get_from();
            continue;
        }
        else
        {
            two = get_from();
        }
        if( one != NULL && two != NULL)
        {
            m = merge(one->word_list,two->word_list);
            one->word_list = m;
            wlist_head->next = m;
            insert_into(one);
            one = two = NULL;
        }
    }
#ifdef TIME_MEGRE
    gettimeofday(&end, NULL);
    usetime(&end, &begin, MEGRE);
#endif
}

void main()
{
    /*read....file*/
    //char *rfile = "./test.txt";
    char *rfile = "./sowpods.txt";
    char *wfile = "./results.txt";
    struct word * word_list = NULL;
    word_list = readfile2list(rfile);
    if(! word_list)
    {
        printf("read file %s error\n",rfile);
        exit(0);
    }

    /*sort single thread*/
    //sortwords(word_list);
    
    /*sort multi thread and merge*/
    sortwords_multi(word_list);

    /*write....file*/
    int ret = -1;
    ret = writenode2file(wfile, word_list);
    if(ret<0)
    {
        printf("write file %s error\n",wfile);
        exit(0);
    }
    return;
}
