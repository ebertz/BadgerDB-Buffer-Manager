/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
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


BufMgr::~BufMgr() {
	for (unsigned i = 0; i < numBufs; i++) {
		BufDesc* buf = &bufDescTable[i];
		if (buf->dirty == 1) flushFile(buf->file);
	}
	free(bufPool);
	free(bufDescTable);
	
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % (numBufs);
}

void BufMgr::allocBuf(FrameId & frame) 
{

	FrameId start = clockHand;
	bool unpinned_frame_exists = false;			
	while(1) {
		advanceClock();
		BufDesc* curBuf = &bufDescTable[clockHand];

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
				std::cout << "FLUSH PAGE " << curBuf->pageNo <<  "from file "  << curBuf->file << "\n" << std::flush;
				curBuf->file->writePage(curBuf->file->readPage(curBuf->pageNo));
				std::cout << "PAGE FLUSHED = " << curBuf->file->readPage(curBuf->pageNo).page_number() << "\n" << std::flush;
				curBuf->dirty = 0;
			}
			hashTable->remove(curBuf->file, curBuf->pageNo);
			curBuf->Print();
			curBuf->Clear();	

			//curBuf->Set(bufDescTable[frame].file, bufDescTable[frame].pageNo);
			curBuf->Print();
			std::cout << "ALLOC FRAME " <<  frame << "\n" << std::flush;		
			return;					

		
		}
		else {
			frame = clockHand;
			//curBuf->Set(bufDescTable[frame].file, bufDescTable[frame].pageNo);
			curBuf->Print();
			std::cout << "ALLOC INVALID FRAME " <<  frame << "\n" << std::flush;		
			return;
		}


	
	}

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo = 0;
	try{
		hashTable->lookup(file, pageNo, frameNo);
		BufDesc* buf = &bufDescTable[frameNo];	
		buf->refbit = 1;
		buf->pinCnt++;
		page = &bufPool[frameNo];
	

	}
	catch(HashNotFoundException) {
		std::cout << "HASH NOT FOUND!!!!!!!!!!!!!\n" << std::flush;		
		allocBuf(frameNo);
		std::cout << frameNo << std::flush;
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);

		BufDesc* buf = &bufDescTable[frameNo];

		buf->Set(file, pageNo);
		page = &bufPool[frameNo];
		buf->Print();
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo);
		BufDesc* buf = &bufDescTable[frameNo];
		if(buf->pinCnt < 1) {
			throw new PageNotPinnedException("filename", pageNo, frameNo);
		}		
		//std::cout << "UNPIN PAGE " << pageNo << " AT FRAME " << frameNo << " PIN COUNT = " << buf->pinCnt;
		buf->pinCnt--;
		//std::cout << " PIN COUNT: " << buf->pinCnt << "\n";
		if(dirty == 1) buf->dirty = 1;

	}
	catch(HashNotFoundException) {std::cout << "HASH NOT FOUND\n";}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	std::cout << "ALLOCATE PAGE\n" << std::flush;
	FrameId frameNo;


	//Page newPage = file->allocatePage();
	//pageNo = newPage.page_number();
	
	allocBuf(frameNo);
	bufPool[frameNo] = file->allocatePage();
	pageNo = bufPool[frameNo].page_number();
	page = &bufPool[frameNo];

	hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);


		
}

void BufMgr::flushFile(const File* file) 
{
	std::cout << "FLUSH\n";
	for(unsigned int i = 0; i < numBufs ; i++)
	{

		BufDesc* desc = &bufDescTable[i];  
		if(desc->file == file)
		{
			if(!desc->valid)
				throw new BadBufferException(desc->frameNo, desc->dirty, desc->valid, desc->refbit);
			if(desc->pinCnt)
				throw new PagePinnedException("filename", desc->pageNo, desc->frameNo);
			if(desc->dirty == 1)
			{
				desc->file->writePage(bufPool[desc->frameNo]);
				desc->dirty = 0;
			}
			hashTable->remove(desc->file, desc->pageNo);
			std::cout << "REMOVED PAGE " << desc->pageNo << "\n" << std::flush;
			desc->Clear();		
			
		}
	}

				std::cout << "RETURN\n" << std::flush;
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	for (unsigned i = 0; i < numBufs; i++) {
		BufDesc* buf = &bufDescTable[i];
		if(buf->file == file && buf->pageNo == PageNo){
			hashTable->remove(buf->file, buf->pageNo);
			buf->valid = 0;	
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
