#ifndef __LFQUEUE_H__
#define __LFQUEUE_H__

#include <thread>
#include <iostream>
#include <atomic>
#include "dtype.h"
#include "os/Win.h"
#include "logger.h"

#define CAS2(obj, expected, desired) std::atomic::atomic_compare_exchange_weak(obj, expected, desired);

#ifdef WIN32
#define CAS(ptr, oldvalue, newvalue) InterlockedCompareExchange(ptr, newvalue, oldvalue)
#else
#define CAS(ptr, oldvalue, newvalue) __sync_val_compare_and_swap(ptr , oldvalue , newvalue)
#endif

typedef struct queue_elem
{
	void*	data;
	bool	origin;		// 0: from GRPC, 1: from Self

	queue_elem(void* e, uint32 o) { data = e; origin = o; }
} QueueElem;

class LockFreeQueue
{
public:
	LockFreeQueue(int32 capicity)
		: capicity_(capicity), header_(0), tailer_(0), guard_(0)
	{
		if (capicity_ < 4) { capicity_ = 4; }
		
		lf_queue_ = new QueueElem*[capicity_];
		for (int i = 0; i < capicity_; i++)
			lf_queue_[i] = NULL;
	}

	~LockFreeQueue() { if (lf_queue_) { delete [] lf_queue_; } }

public:
	enum QStatus {
		Empty,
		Full,
		Normal,
		Unknown
	};

	//bool isempty() { return header_ == tailer_; }
	// guard_ is the maximum dequeue item
	bool isempty() { return header_ == guard_; }

	bool isfull() { return (internal_index(tailer_ + 1)) == header_; }

	int32 internal_index(int32 v) { return (v % capicity_); }

	bool enqueue(QueueElem* item)
	{
		int32 temp,guard;

		assert(item);

		do
		{
			// fetch tailer_ first and then judge isfull, else encounter concurrent problem
			temp = tailer_;
			guard = guard_;

			if (isfull())
			{
				return false;
			}

			// cross operate 
			if (CAS(&tailer_, temp, internal_index(temp + 1)) == temp)
			{
				lf_queue_[temp] = item;

				// update the guard_ for the max dequeue item
				CAS(&guard_, guard, internal_index(guard + 1));
				break;
			}
			else
			{
				//std::cout << "enqueue Cas failure one times" << std::endl;
			}
		} while (true);

		return true;
	}

	bool dequeue(QueueElem** item)
	{
		int32 temp;

		do
		{
			// fetch header first and then judge isempty, else encounter concurrent problem
			temp = header_;
			*item = NULL;

			if (isempty())
			{
				return false;
			}

			// cross operate CAS failure
			*item = lf_queue_[temp];
			if (CAS(&header_, temp, internal_index(temp + 1)) == temp)
			{
				// some producer lock one slot, but doesn't push back
// 				while (!lf_queue_[temp])
// 				{
// 					std::this_thread::yield();
// 				}

				//*item = lf_queue_[temp];
				lf_queue_[temp] = NULL;
				break;
			}
			else
			{
				//std::cout << "dequeue Cas failure one times" << std::endl;
			}
		} while (true);

		return true;
	}

private:
	QueueElem**	lf_queue_;
	int32			capicity_;
#ifdef WIN32
	LONG			header_;
	LONG			tailer_;
	LONG			guard_;			// header <= guard <= tailer 
#else
	int32			header_;
	int32			tailer_;
	int32			guard_;			// header <= guard <= tailer
#endif
};



#endif