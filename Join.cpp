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

    vector<Bucket> partitions((MEM_SIZE_IN_PAGE - 1), Bucket(disk));  // placeholder
    for (int i = left_rel.first; i < left_rel.second; ++i) {
        mem->loadFromDisk(disk, i, 0);
        Page* page = mem->mem_page(0);
        for (int r = 0; r < page->size(); ++r) {
            Record rec = page->get_record(r);
            int hash = rec.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            cout << hash << " ";
            rec.print();
        }
    }

    for (int i = right_rel.first; i < right_rel.second; ++i) {
        mem->loadFromDisk(disk, i, 0);
        Page* page = mem->mem_page(0);
        for (int r = 0; r < page->size(); ++r) {
            Record rec = page->get_record(r);
            int hash = rec.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            cout << hash << " ";
            rec.print();
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
    vector<uint> disk_pages;  // placeholder
    return disk_pages;
}
