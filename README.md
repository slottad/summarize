##summarize
==========

###Usage Example:

**iquery -aq "summarize(A)"

{instance_id,attribute_id} att_name,total_count,total_bytes,num_chunks,min_count,avg_count,max_count,min_bytes,avg_bytes,max_bytes**

{0,0} 'a',11,177,1,11,11,11,177,177,177

{0,1} 'c',11,176,1,11,11,11,176,176,176

{0,2} 'val_avg',11,160,1,11,11,11,160,160,160

{0,3} 'EmptyTag',11,48,1,11,11,11,48,48,48

{1,0} 'a',3,102,1,3,3,3,102,102,102

{1,1} 'c',3,96,1,3,3,3,96,96,96

{1,2} 'val_avg',3,96,1,3,3,3,96,96,96

{1,3} 'EmptyTag',3,48,1,3,3,3,48,48,48

{2,0} 'a',11,144,1,11,11,11,144,144,144

{2,1} 'c',11,160,1,11,11,11,160,160,160

{2,2} 'val_avg',11,160,1,11,11,11,160,160,160

{2,3} 'EmptyTag',11,48,1,11,11,11,48,48,48

{3,0} 'a',8,153,1,8,8,8,153,153,153

{3,1} 'c',8,152,1,8,8,8,152,152,152

{3,2} 'val_avg',8,136,1,8,8,8,136,136,136

{3,3} 'EmptyTag',8,48,1,8,8,8,48,48,48

{4,0} 'a',7,144,1,7,7,7,144,144,144

{4,1} 'c',7,128,1,7,7,7,128,128,128

{4,2} 'val_avg',7,128,1,7,7,7,128,128,128

{4,3} 'EmptyTag',7,48,1,7,7,7,48,48,48

**iquery -aq "summarize(A,'per_instance=1')"

{instance_id,attribute_id} att_name,total_count,total_bytes,num_chunks,min_count,avg_count,max_count,min_bytes,avg_bytes,max_bytes**

{0,0} '',44,561,4,11,11,11,48,140.25,177

{1,0} '',12,342,4,3,3,3,48,85.5,102

{2,0} '',44,512,4,11,11,11,48,128,160

{3,0} '',32,489,4,8,8,8,48,122.25,153

{4,0} '',28,448,4,7,7,7,48,112,144

**iquery -aq "summarize(A,'per_attribute=1')"

{instance_id,attribute_id} att_name,total_count,total_bytes,num_chunks,min_count,avg_count,max_count,min_bytes,avg_bytes,max_bytes**

{0,0} 'a',40,720,5,3,8,11,102,144,177

{0,1} 'c',40,712,5,3,8,11,96,142.4,176

{0,2} 'val_avg',40,680,5,3,8,11,96,136,160

{0,3} 'EmptyTag',40,240,5,3,8,11,48,48,48



