1 read file from dist into a array of struct word
2 cut the array into sereval parts , based on thread num
3 each working thread sort the words belong to it 
4 the master thread merge the result of each working thread
5 write the result into file on dist
