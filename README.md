fast_count
==========

Usage Example:
iquery -aq "fast_count(A)"




How does the fast count handle overlapping arrays?

In SciDB, in "Array.cpp: ConstChunk* ConstChunk::materialize() const "


if (!getArrayDesc().hasOverlap()) {
                    materializedChunk->setCount(count);
                                }

Therefore, the count is only set if there array does not have overlap. If the count is not set, the chunks are counted
via an iterator.  
