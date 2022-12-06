#include "Join.hpp"

#include <iostream>
#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1,
	                          Bucket(disk)); // partition

	for (uint i = left_rel.first; i < left_rel.second; ++i) {
		mem->loadFromDisk(disk, i, 0);
		Page* page = mem->mem_page(0);
		for (uint r = 0; r < page->size(); ++r) {
			Record rec = page->get_record(r);
			uint index = 1
			        + rec.partition_hash()
			                % (MEM_SIZE_IN_PAGE - 1); // bucket index

			Page* bufferPage = mem->mem_page(index);
			if (bufferPage->full()) {
				uint disk_page_id = mem->flushToDisk(disk, index);
				partitions[index].add_left_rel_page(disk_page_id);
			}
			bufferPage->loadRecord(rec);
		}
	}

	for (uint i = 1; i < MEM_SIZE_IN_PAGE; ++i) {
		Page* page = mem->mem_page(i);
		if (!page->empty()) {
			uint disk_page_id = mem->flushToDisk(disk, i);
			partitions[i - 1].add_left_rel_page(disk_page_id);
		}
	}

	for (uint i = right_rel.first; i < right_rel.second; ++i) {
		mem->loadFromDisk(disk, i, 0);
		Page* page = mem->mem_page(0);
		for (uint r = 0; r < page->size(); ++r) {
			Record rec = page->get_record(r);
			uint index = 1
			        + rec.partition_hash()
			                % (MEM_SIZE_IN_PAGE - 1); // bucket index

			Page* bufferPage = mem->mem_page(index);
			if (bufferPage->full()) {
				uint disk_page_id = mem->flushToDisk(disk, index);
				partitions[index].add_right_rel_page(disk_page_id);
			}
			bufferPage->loadRecord(rec);
		}
	}

	for (uint i = 1; i < MEM_SIZE_IN_PAGE; ++i) {
		Page* page = mem->mem_page(i);
		if (!page->empty()) {
			uint disk_page_id = mem->flushToDisk(disk, i);
			partitions[i - 1].add_right_rel_page(disk_page_id);
		}
	}

	return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder

	Page* write_page = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
	for (auto& b : partitions) {
		vector<uint> left_disk_ids = b.get_left_rel();

		for (uint i : left_disk_ids) {
			mem->loadFromDisk(disk, i, 0);
			Page* input = mem->mem_page(0);
			for (uint r = 0; r < input->size(); ++r) {
				Record rec_s = input->get_record(r);
				uint index = 1
				        + rec_s.probe_hash()
				                % (MEM_SIZE_IN_PAGE - 2); // bucket index
				Page* bucket = mem->mem_page(index);
				bucket->loadRecord(rec_s);
			}
		}

		vector<uint> right_disk_ids = b.get_right_rel();
		for (uint i : right_disk_ids) {
			mem->loadFromDisk(disk, i, 0);
			Page* input = mem->mem_page(0);
			for (uint s = 0; s < input->size(); ++s) {
				Record recS = input->get_record(s);
				uint index = 1
				        + recS.probe_hash()
				                % (MEM_SIZE_IN_PAGE - 2); // bucket index
				Page* bucket = mem->mem_page(index);
				for (uint r = 0; r < bucket->size(); ++r) {
					Record recR = bucket->get_record(r);
					if (recR == recS) {
						if (write_page->full()) {
							uint disk_page_id = mem->flushToDisk(
							        disk, MEM_SIZE_IN_PAGE - 1);
							disk_pages.push_back(disk_page_id);
							write_page->reset();
						}
						write_page->loadPair(recR, recS);
					}
				}
			}
		}
	}

	if (!write_page->empty()) {
		uint disk_page_id = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
		disk_pages.push_back(disk_page_id);
		write_page->reset();
	}

	return disk_pages;
}
