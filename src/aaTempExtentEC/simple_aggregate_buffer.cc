#include "simple_aggregate_buffer.h"



int SimpleAggregateBuffer::write(MOSDOp* op, const OSDMap &osdmap)
{   
    SimpleVolume* last_vol = nullptr;
    int ret = 0;
    if(!volumes.empty()) {
        // TODO：这里可以通过某种方式（如定时器历史调整信息）来判断是否是批量写入
        // this->may_batch_put()
        
        if (may_batch_writing()) {
            SimpleVolume* last_vol = volumes.back();
        }
        else {
            // 逆序遍历volume list
            std::list<SimpleVolume*>::iterator i = volumes.rbegin();
            while(i != volumes.rend()) {
                if (!(*i)->full()) {
                    last_vol = *i;
                    break;
                }
                i++;
            }
        }
    
    } else {
        last_vol = new SimpleVolume(SimpleVolume::default_volume_capacity, this);
    }

    if (last_vol) {
        r = last_vol->add_chunk(op, osdmap);
        if (r < 0) {
            cout << "failed to add chunk in volume." << std::endl;
            r = CHUNK_FAILED;
        }
    } else {
        cout << "failed to get volume." << std::endl;
        r = VOLUME_FAILED;
    }

    return r;
}