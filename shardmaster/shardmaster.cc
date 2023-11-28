#include "shardmaster.h"


::grpc::Status StaticShardmaster::Join(::grpc::ServerContext* context,
                                       const ::JoinRequest* request,
                                       Empty* response) {
  //empty
  int count = mapp.size();
  mtx.lock();
  if (count == 0)
  {
    shard_t temp_shard;
    temp_shard.lower = MIN_KEY;
    temp_shard.upper = MAX_KEY;

    server_shard server;
    server.name = request->server();
    server.number = 1;
    server.shard_v.push_back(temp_shard);

    mapp.push_back(server);
    mtx.unlock();
    return ::grpc::Status::OK;
  }
  //none empty
  else
  {
    //none empty, check already exist
    for(int i = 0; i < count ; i++)
    {
      if (mapp[i].name == request->server())
      {
        mtx.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: master_join already exist>");
      }
    }
    //none empty, none exist, join
    //join a new server
    shard_t temp_shard;
    temp_shard.lower = MIN_KEY;
    temp_shard.upper = MAX_KEY;
    server_shard server;
    server.name = request->server();
    server.number = 1;
    server.shard_v.push_back(temp_shard);
    mapp.push_back(server);

    int num_servers = count + 1;
    // printf("%d\n", num_servers);
    //change old server's shard
    int num_bigger = (MAX_KEY - MIN_KEY + 1) - int((MAX_KEY - MIN_KEY + 1) / num_servers) * num_servers; 
    int step = int((MAX_KEY - MIN_KEY + 1) / num_servers);
    int start = 0;
    int end = -1; 
    for (int i = 0; i < num_servers; i++)
    {
      if(i < num_bigger)
      {
        mapp[i].shard_v.clear();
        start = end + 1;
        end = start + step;
        shard_t temp_shard;
        temp_shard.lower = start;
        temp_shard.upper = end;
        mapp[i].shard_v.push_back(temp_shard);
      }
      else
      {
        mapp[i].shard_v.clear();
        start = end + 1;
        end = start + step - 1;
        shard_t temp_shard;
        temp_shard.lower = start;
        temp_shard.upper = end;
        mapp[i].shard_v.push_back(temp_shard);
      }
    }
    mtx.unlock();
    return ::grpc::Status::OK;
  }

  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}


::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext* context,
                                        const ::LeaveRequest* request,
                                        Empty* response) {
  //empty
  int count = mapp.size();
  int num_leave = request->servers_size();
  mtx.lock();
  if (count == 0)
  {
    mtx.unlock();
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: master_leave  empty>");
  }
  //none empty
  else
  {
    for(int i = 0; i < num_leave; i++)
    {
      std::string leave_server = request->servers(i);
      //none empty, check exist, leave
      int find = 0;
      for(int j = 0; j < count; j++)
      {
        if (mapp[j].name == leave_server)
        {
          mapp.erase(mapp.begin() + j);
          find = 1;
        }
      }
      count -= find;
      //none empty, don't exist, error
      if(find == 0)
      {
        mtx.unlock();
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: master_leave don't exist>"); 
      }

    }
    //restruct shard
    if (count == 0)
    {
      mtx.unlock();
      return ::grpc::Status::OK;
    }
    //none empty
    else
    {
    int num_servers = count;
    //change old server's shard
    int num_bigger = (MAX_KEY - MIN_KEY + 1) - int((MAX_KEY - MIN_KEY + 1) / num_servers) * num_servers; 
    int step = int((MAX_KEY - MIN_KEY + 1) / num_servers);
    int start = 0;
    int end = -1; 
    for (int i = 0; i < num_servers; i++)
    {
      if(i < num_bigger)
      {
        mapp[i].shard_v.clear();
        start = end + 1;
        end = start + step;
        shard_t temp_shard;
        temp_shard.lower = start;
        temp_shard.upper = end;
        mapp[i].shard_v.push_back(temp_shard);
      }
      else
      {
        mapp[i].shard_v.clear();
        start = end + 1;
        end = start + step - 1;
        shard_t temp_shard;
        temp_shard.lower = start;
        temp_shard.upper = end;
        mapp[i].shard_v.push_back(temp_shard);
      }
    }
    mtx.unlock();
    return ::grpc::Status::OK;

    }

  }

}


::grpc::Status StaticShardmaster::Move(::grpc::ServerContext* context,
                                       const ::MoveRequest* request,
                                       Empty* response) {
  // Hint: Take a look at get_overlap in common.{h, cc}
  // Using the function will save you lots of time and effort!
  mtx.lock();
  std::string des = request->server();
  int start = request->shard().lower();
  int end = request->shard().upper();
  int num_v = mapp.size();
  shard_t bb;
  bb.upper = request->shard().upper();
  bb.lower = request->shard().lower();
  int find = 0;
  for(int i = 0; i < num_v; i++)
  {
    if(mapp[i].name == des)
    {
      find = 1;
    }
  }
  if(find == 0)
  {
    mtx.unlock();
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<error: master_move target don't exist>"); 
  }
  for(int i = 0; i < num_v; i++)
  {
    int shard_size = mapp[i].shard_v.size();
    for(int j = 0; j < shard_size; j++)
    {
      
      if(get_overlap(mapp[i].shard_v[j],bb) == OverlapStatus::COMPLETELY_CONTAINED)
      {
        // printf("erase name:%s, i is:%d, j is:%d, %d %d\n", mapp[i].name,i, j, mapp[i].shard_v[j].lower,mapp[i].shard_v[j].upper );
        // mapp.erase(mapp.begin() + i);
        mapp[i].shard_v.erase(mapp[i].shard_v.begin() + j);
        // mapp[i].shard_v.erase(shard_v.begin() + j);
      }
      if(get_overlap(mapp[i].shard_v[j],bb) == OverlapStatus::COMPLETELY_CONTAINS)
      {
        shard_t temp;
        temp.lower = end + 1;
        temp.upper = mapp[i].shard_v[j].upper;
        mapp[i].shard_v[j].upper = start - 1;
        mapp[i].shard_v.push_back(temp);
      }
      if(get_overlap(mapp[i].shard_v[j],bb) == OverlapStatus::OVERLAP_START)
      {
        mapp[i].shard_v[j].lower = end + 1;
      }
      if(get_overlap(mapp[i].shard_v[j],bb) == OverlapStatus::OVERLAP_END)
      {
        mapp[i].shard_v[j].upper = start - 1;
      }
    // printf("name:%s, i is:%d, j is:%d, %d %d\n", mapp[i].name,i, j, mapp[i].shard_v[j].lower,mapp[i].shard_v[j].upper );
    }
    sortAscendingInterval(mapp[i].shard_v);
    if(mapp[i].name == des)
    {
      mapp[i].shard_v.push_back(bb);
    }
  }
  
  mtx.unlock();
  return ::grpc::Status::OK;
  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}


::grpc::Status StaticShardmaster::GDPRDelete(::grpc::ServerContext* context,
                                           const ::GDPRDeleteRequest* request,
                                           Empty* response) {
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}


::grpc::Status StaticShardmaster::Query(::grpc::ServerContext* context,
                                        const StaticShardmaster::Empty* request,
                                        ::QueryResponse* response) {
  int num_v = mapp.size();
  mtx.lock();
  int find = 0;
  for(int i = 0; i < num_v; i ++)
  {
    ConfigEntry* entry = response->add_config();
    // ConfigEntry entry = response->config(i);
    entry->set_server(mapp[i].name);
    // response->config(i).set_server(mapp[i].name);
    int shard_size = mapp[i].shard_v.size();
    for (int j = 0; j < shard_size; j++)
    {
      // Shard* s = response->config(i).add_shards();
      Shard* s = entry->add_shards();
      // s->set_server(mapp[i].name);
      s->set_lower(mapp[i].shard_v[j].lower);
      s->set_upper(mapp[i].shard_v[j].upper);
    }
    
  }
  mtx.unlock();
  return ::grpc::Status::OK;
  // return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "not implemented");
}
