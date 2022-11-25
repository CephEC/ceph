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
            // TODO：查volume元数据
            // 先逆序遍历volume list看是否有未往下写的未满volume
            std::list<SimpleVolume*>::iterator i = volumes.rbegin();
            while(i != volumes.rend()) {
                if (!(*i)->full() && !(*i)->flushing()) {
                    last_vol = *i;
                    break;
                }
                i++;
            }
            
            // buffer中没有空闲vol，查找volume元数据缓存
            if (i == volumes.rend()) {
                std::vector<volume_t>::iterator vol = volume_meta_cache.begin();
                while (vol != volume_meta_cache.end()) {
                    if (!vol.full()) {
                        // TODO: 根据元数据读取数据块和EC块到osd，构建volume
                        // 是在这一步读取还是延迟读取？元数据一致性如何保证？

                        last_vol = new SimpleVolume(SimpleVolume::default_volume_capacity, this);
                        // add_chunk 

                    }
                }

            }
        }
    
    }

    // 卷已满（针对批量写入情况的末尾卷）或者为空
    if(!last_vol || last_vol->full()) {
        last_vol = new SimpleVolume(SimpleVolume::default_volume_capacity, this);
    }
    
    // 向卷中添加chunk
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