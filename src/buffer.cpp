/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * 
 * Name: Chance Hammacher
 * Student ID: 9070118170
 *
 * Name: Connor Hoffmann
 * Student ID: 9070374252
 * 
 * Name: Evan Bertz
 * Student ID: 9070334721
 *
 * This file is used to implement the clock algo for a buffer manager
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

//
//Buffer Manager Deconstructor. Flushes Dirty pags and deallocates the buffer pool and description table
//
BufMgr::~BufMgr() {
	//check for any dirty pages that need to be flushed to disk
	for (unsigned i = 0; i < numBufs; i++) {
		BufDesc* buf = &bufDescTable[i];
		if (buf->dirty == 1){
			buf->file->writePage(bufPool[buf->frameNo]);
		}
	}

	delete[] bufPool;
	delete[] bufDescTable;
	delete hashTable;	
}


//Advances clock to the next frame in pool
void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % (numBufs);
}


//Allocates a free frame and writes dirty pages back to disk. 
// Return: a pointer to the frame number used.
void BufMgr::allocBuf(FrameId & frame) 
{

	FrameId start = clockHand;
	// use this to check if no pages have been pinned
	bool unpinned_frame_exists = false;			
	while(1) {
		// advances clock each iteration of loop.
		advanceClock();
		BufDesc* curBuf = &bufDescTable[clockHand];

		// if clock has gone around in a full loop check if pages pinned
		if (clockHand == start) {
			if (!unpinned_frame_exists) throw BufferExceededException();
			else unpinned_frame_exists = false;

		}
		
		frame = clockHand;
		//valid set? -- if true, set() frame
		if(curBuf->pinCnt < 1) unpinned_frame_exists = true;
		if(curBuf->valid) {
	
			//refbit set? -- if true, clear refbit
			if(curBuf->refbit){
				curBuf->refbit = 0;
				continue;
			}
			//page pinned? -- if true, advance clock pointer
			if (curBuf->pinCnt > 0) {
				continue;
			}

			//dirty bit set? --if true flush page to disk
			if(curBuf->dirty)
			{	
				curBuf->file->writePage(bufPool[clockHand]);
			}
			hashTable->remove(curBuf->file, curBuf->pageNo);
			curBuf->Clear();	
			return;					

		
		}
		else {
			//if not valid just clear frame and return
			frame = clockHand;
			curBuf->Clear();
			return;
		}


	
	}

}

//Reads a page thats in the buffer or adds to buffer if it needed
//Input: file: points to a file to be read, pageNo: the page number in the file
//returns pointer to page if it is in the buffer pool
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try{
		//looks for page in hashtable, may throw exception
		hashTable->lookup(file, pageNo, frameNo);
	}
	catch(HashNotFoundException) {
		//if not in hashtable, find frame to add too
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);

		BufDesc* buf = &bufDescTable[frameNo];

		buf->Set(file, pageNo);
		page = &bufPool[frameNo];
		return;
	}
	// if page was in hashtable increase pin count and set refbit
	BufDesc* buf = &bufDescTable[frameNo];
        buf->refbit = 1;
        buf->pinCnt++;
        page = &bufPool[frameNo];
}


//if page is in hashtable, decrement the pincount
//Input: file: a pointer to a file, pageNo: page number of file to be looked at, dirty: a bool used to set a page unPined to true
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try {
		//looks for page in the hashTable, may throw exception
		hashTable->lookup(file, pageNo, frameNo);
	}
	catch(HashNotFoundException) {
		// do nothing if it wasn't found
		return;
	}

	BufDesc* buf = &bufDescTable[frameNo];
	//check dirty bit passed in and set the dirty bit of frame if needed
	if(dirty == true) {buf->dirty = true;}
	//if pin count is already 0 throw exception
        if(buf->pinCnt < 1) {
                throw PageNotPinnedException("filename", pageNo, frameNo);
        }
	// decrease pin count
        buf->pinCnt--;
}


//Returns a newly allocated page and inserts into hastable and is set accordingly
//Input: file: a file to use for allocating
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNo;

	//finds a frame to be used to alloc a Page
	allocBuf(frameNo);
	bufPool[frameNo] = file->allocatePage();
	pageNo = bufPool[frameNo].page_number();
	page = &bufPool[frameNo];

	hashTable->insert(file, bufPool[frameNo].page_number(), frameNo);
	bufDescTable[frameNo].Set(file,bufPool[frameNo].page_number());


		
}


//Writes dirty pages to disk 
//Input: file: a file to remove from disk
void BufMgr::flushFile(const File* file) 
{
	//check each frame to find a file and page
	for(unsigned int i = 0; i < numBufs ; i++)
	{

		BufDesc* desc = &bufDescTable[i]; 
		// if there is a file and page 
		if(desc->file == file)
		{
			if(!desc->valid)
				throw BadBufferException(desc->frameNo, desc->dirty, desc->valid, desc->refbit);
			if(desc->pinCnt)
				throw PagePinnedException("filename", desc->pageNo, desc->frameNo);
			// if its dirty flush the page
			if(desc->dirty == 1)
			{
				desc->file->writePage(bufPool[desc->frameNo]);
				desc->dirty = 0;
			}
			// remove it from hashTable
			hashTable->remove(desc->file, desc->pageNo);
			desc->Clear();		
			
		}
	}
}


//deletes a page from the file and frees space accodingly
//Input: file: a file containing a page, PageNo: the page number in a file to remove from that file input
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	//check if the current page is in the buffer
	for (unsigned i = 0; i < numBufs; i++) {
		BufDesc* buf = &bufDescTable[i];
		if(buf->file == file && buf->pageNo == PageNo){
			hashTable->remove(buf->file, buf->pageNo);
			buf->Clear();
		}
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
