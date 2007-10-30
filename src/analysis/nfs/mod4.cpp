/*
   (c) Copyright 2003-2007, Hewlett-Packard Development Company, LP

   See the file named COPYING for license details
*/

#include <list>

#include <Lintel/HashTable.H>
#include <Lintel/PriorityQueue.H>
#include <Lintel/StringUtil.H>

#include "analysis/nfs/mod4.hpp"

using namespace std;
class ServersPerFilehandle : public NFSDSModule {
public:
  ServersPerFilehandle(DataSeriesModule &_source)
	: source(_source),
	  server(s,"server"),
	  filehandle(s,"filehandle"),
	  filename(s,"filename",Field::flag_nullable),
	  type(s,"type"),
	  filesize(s,"file-size"),
	  modifytime(s,"modify-time"),
	  record_id(s,"record-id")
    { }
    virtual ~ServersPerFilehandle() { }

  struct svrData {
    ExtentType::int32 server;
    Stats *size;
    Stats *modify;
  };

    struct hteData {
      vector<svrData> servers;
      string filehandle, filename, type;
      ExtentType::int64 record_id;
    };

    class hteHash {
    public:
	unsigned int operator()(const hteData &k) {
	    return HashTable_hashbytes(k.filehandle.data(),k.filehandle.size());
	}
    };

    class hteEqual {
    public:
	bool operator()(const hteData &a, const hteData &b) {
	    return a.filehandle == b.filehandle;
	}
    };
    HashTable<hteData, hteHash, hteEqual> stats_table;

    virtual Extent *getExtent() {
      unsigned int i;
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	AssertAlways(e->type.getName() == "attr-ops-join",("bad\n"));

	hteData k;
	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    k.filehandle = filehandle.stringval();

	    hteData *v = stats_table.lookup(k);

	    if (v == NULL) {
	      svrData news;
	      news.server = server.val();
	      news.size = new Stats;
	      news.size->add(filesize.val());
	      news.modify = new Stats;
	      news.modify->add(modifytime.val());
	      k.servers.clear();
	      k.servers.push_back(news);
	      k.type = type.stringval();
	      k.record_id = record_id.val();
	      v = stats_table.add(k);
	      //	      printf("A %s 0 %u %x %x %d\n", 
	      //	     hexstring(v->filehandle).c_str(),
	      //	     v->servers.size(),v->servers[0], server.val(),
	      //	     v->servers[0]==server.val()?1:0);
	    }

		for(i = 0; i < v->servers.size(); ++i) {
		  //  printf("B %s %u %u %x %x %d\n", 
		  //	 hexstring(v->filehandle).c_str(),
		  //	 i, v->servers.size(),v->servers[i], server.val(),
		  //	 v->servers[i]==server.val()?1:0);
		  if(v->servers[i].server == server.val())
		    break;
		}
		if(i >= v->servers.size()) {
		  // did not find server
		  //		  printf("%s %s %lld %s %lld\n",
		  //			 hexstring(v->filehandle).c_str(),
		  //			 ipstring(v->servers[0].server).c_str(),
		  //			 v->record_id,
		  //			 ipstring(server.val()).c_str(),
		  //			 record_id.val());
		  svrData news;
		  news.server = server.val();
		  news.size = new Stats;
		  news.size->add(filesize.val());
		  news.modify = new Stats;
		  news.modify->add(modifytime.val());
		  v->servers.push_back(news);
		} else {
		  // update filesize
		  v->servers[i].size->add(filesize.val());
		  // update modify time
		  v->servers[i].modify->add(modifytime.val());
		}
		
	    if (v->filename.empty() == true && filename.isNull() == false) {
		v->filename = filename.stringval();
	    }
	}
	return e;
    }
    
    class sortByNumServers {
    public:
	bool operator()(hteData *a, hteData *b) {
	    return a->servers.size() > b->servers.size();
	}
    };
    
    virtual void printResult() {
      //	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	vector<hteData *> vals;
	for(HashTable<hteData, hteHash, hteEqual>::iterator i = stats_table.begin();
	    i != stats_table.end();++i) {
	    vals.push_back(&(*i));
	}
	sort(vals.begin(),vals.end(),sortByNumServers());

	for(vector<hteData *>::iterator i=vals.begin(); i!=vals.end(); ++i) {
	  hteData *j = *i;

	  printf("%s %d %d",
		 hexstring(j->filehandle).c_str(),
		 j->filehandle.size(),
		 j->servers.size());

	  for(unsigned int k=0; k<j->servers.size(); ++k) {
	    printf(" %s %.0f %.0f %.0f %.0f",
		   ipv4tostring(j->servers[k].server).c_str(),
		   j->servers[k].size->min(),
		   j->servers[k].size->max(),
		   j->servers[k].modify->min(),
		   j->servers[k].modify->max());
	  }
	  printf("\n");
	}

	//	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
    
    DataSeriesModule &source;
    ExtentSeries s;
    Int32Field server;
    Variable32Field filehandle,filename,type;
    Int64Field filesize, modifytime, record_id;
};



NFSDSModule *
NFSDSAnalysisMod::newServersPerFilehandle(DataSeriesModule &prev)
{
    return new ServersPerFilehandle(prev);
}


#if 0
class Transactions : public NFSDSModule {
public:
    static const bool keeptimelist = false;

    Transactions(DataSeriesModule &_source) 
	: source(_source), s(ExtentSeries::typeExact),
	  reqtime(s,"packet-at"),
	  sourceip(s,"source"),destip(s,"dest"),	  
	  is_udp(s,"is-udp"),
	  is_request(s,"is-request"),
          transaction_id(s, "transaction-id"),
	  op_id(s,"op-id",Field::flag_nullable),
	  operation(s,"operation")
    {}

    virtual ~Transactions() { }

    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field reqtime;
    Int32Field sourceip, destip;
    BoolField is_udp;
    BoolField is_request;
    Int32Field transaction_id;
    ByteField op_id;
    Variable32Field operation;

  // data structure to keep requests that do not yet have a matching response
    struct tidData {
      unsigned tid, server, client;
      unsigned long long reqtime;
      string operation;

      tidData(unsigned tid_in, unsigned server_in, unsigned client_in) :
	tid(tid_in), server(server_in), client(client_in), 
	reqtime(0), operation("") {}
    };

  class tidHash {
  public: unsigned int operator()(const tidData &t) {
    return t.tid ^ t.client;
  }};
  class tidEqual {
  public: bool operator()(const tidData &t1, const tidData &t2) {
    return t1.tid == t2.tid && t1.client == t2.client;
  }};

    HashTable<tidData, tidHash, tidEqual> pending;

    virtual Extent *getExtent() {
	Extent *e = source.getExtent();
	if (e == NULL) 
	    return NULL;
	if (e->type.getName() != "NFS trace: common")
	    return e;

	for(s.setExtent(e);s.pos.morerecords();++s.pos) {
	    if (op_id.isNull())
		continue;
	    if (is_request.val()) {
		tidData dummy(transaction_id.val(), destip.val(), sourceip.val()); // address of server = destip
		tidData *t = pending.lookup(dummy);
		if (t == NULL) {
		    // add request to list of pending requests (requests without a response yet)
		    dummy.reqtime = reqtime.val();
		    dummy.operation = operation.stringval();

		    t = pending.add(dummy);
		} else {
		    // request is a retransmit; keep the request time of the first request
		    // for statistics
		}
	    } else { 
		// row is a response
		tidData dummy(transaction_id.val(), sourceip.val(), destip.val()); // address of server = sourceip
		tidData *t = pending.lookup(dummy);
		if (t == NULL) {
		    // response without request
		  printf("NO_REQ %s %s %u %lld\n",
			 ipstring(destip.val()).c_str(),
			 ipstring(sourceip.val()).c_str(),
			 transaction_id.val(),
			 reqtime.val());
		} else {
		    // we now have both request and response
		  int delay = (reqtime.val()-t->reqtime)/1000000;
		    printf("DONE %s %s %u %lld %lld %d %s\n",
			   ipstring(destip.val()).c_str(),
			   ipstring(sourceip.val()).c_str(),
			   transaction_id.val(),
			   t->reqtime/1000000,
			   reqtime.val()/1000000,
			   delay,
			   t->operation.c_str());

		    // remove request from pending hashtable
		    pending.remove(*t);
		}
	    }
	}
	return e;
    }

    virtual void printResult() {
	printf("Begin-%s\n",__PRETTY_FUNCTION__);
	for(HashTable<tidData, tidHash, tidEqual>::iterator i =
                                               pending.begin();
	    i != pending.end();
	    ++i)
	{
		  printf("NO_RSP %s %s %u %lld %s\n",
			 ipstring(i->client).c_str(),
			 ipstring(i->server).c_str(),
			 i->tid,
			 i->reqtime,
			 i->operation.c_str());

	}
	printf("End-%s\n",__PRETTY_FUNCTION__);
    }
};

NFSDSModule *
NFSDSAnalysisMod::newTransactions(DataSeriesModule &prev)
{
    return new Transactions(prev);
}
#endif

class Transactions : public NFSDSModule {
public:
    static const bool keeptimelist = false;

    Transactions(DataSeriesModule &_source) 
	: source(_source), s(ExtentSeries::typeExact),
	  reqtime(s,"packet-at"),
	  sourceip(s,"source"),destip(s,"dest"),	  
	  is_udp(s,"is-udp"),
	  is_request(s,"is-request"),
          transaction_id(s, "transaction-id"),
	  op_id(s,"op-id",Field::flag_nullable),
	  operation(s,"operation")
    {}

    virtual ~Transactions() { }

    DataSeriesModule &source;
    ExtentSeries s;
    Int64Field reqtime;
    Int32Field sourceip, destip;
    BoolField is_udp;
    BoolField is_request;
    Int32Field transaction_id;
    ByteField op_id;
    Variable32Field operation;
  Stats Complete, NoRequest, NoResponse, RetransmitRsp;

  // data structure for prioritizing requests
  struct reqinfo { 
    ExtentType::int64 reqtime;
    ExtentType::int32 sourceip, destip;
    ExtentType::int32 transactionid;
    string operation;
    };

    struct reqinfo_geq { 
	bool operator()(const reqinfo *a, const reqinfo *b) {
	    return a->reqtime > b->reqtime;
	}
    };
    PriorityQueue<reqinfo *,reqinfo_geq> req_reorder;


  // data structure to keep responses until we have sorted requests
  struct rspData {
    ExtentType::int64 rsptime;
    ExtentType::int32 sourceip, destip;
    ExtentType::int32 transactionid;
    string operation;
  };

  class rspHash {
  public: unsigned int operator()(const rspData &r) {
    return r.transactionid ^ r.destip;
  }};

  class rspEqual {
  public: bool operator()(const rspData &r1, const rspData &r2) {
    return r1.transactionid == r2.transactionid && r1.destip == r2.destip;
  }};

  HashTable<rspData, rspHash, rspEqual> pending;

  virtual Extent *getExtent() {
    Extent *e = source.getExtent();
    if (e == NULL) 
      return NULL;
    if (e->type.getName() != "NFS trace: common")
      return e;

    for(s.setExtent(e);s.pos.morerecords();++s.pos) {
      if (op_id.isNull())
	continue;

      if (is_request.val()) {
	// add request to priority queue

	reqinfo *tmp = new reqinfo;
	tmp->reqtime = reqtime.val();
	tmp->sourceip = sourceip.val();
	tmp->destip = destip.val();
	tmp->transactionid = transaction_id.val();
	tmp->operation = operation.stringval();
	
	req_reorder.push(tmp);
      }
      else { 
	// response
	rspData dummy;
	dummy.rsptime = reqtime.val();
	dummy.sourceip = sourceip.val();
	dummy.destip = destip.val();
	dummy.transactionid = transaction_id.val();
	dummy.operation = operation.stringval();
	
	rspData *t = pending.lookup(dummy);
	if (t == NULL) {
	  // add to hashtable of pending responses
	  t = pending.add(dummy);
	} else {
	  // response is a retransmit
	  RetransmitRsp.add(1);
	} 
      }

      // check for completed transactions
      Extent::int64 age;
      if(req_reorder.empty() == false)
	age=(reqtime.val()-req_reorder.top()->reqtime)/1000000000;
      else
	age=0;

      while(req_reorder.empty() == false &&
	    s.pos.morerecords() &&
	    (age >= 10)) {

	// check for a matching response
	rspData tmp;
	tmp.destip = req_reorder.top()->sourceip;
	tmp.transactionid = req_reorder.top()->transactionid;

	rspData *t = pending.lookup(tmp);
	if(t == NULL) {
	  // no matching response found; drop request
	  NoResponse.add(1);
#if 0
	  printf("NO_RSP %s %s %x %lld %lld %lld %lld\n",
		 ipstring(req_reorder.top()->sourceip).c_str(),
		 ipstring(req_reorder.top()->destip).c_str(),
		 req_reorder.top()->transactionid,
		 req_reorder.top()->reqtime,
		 reqtime.val(),
		 reqtime.val() - req_reorder.top()->reqtime,
		 age);	  
#endif
	} else {
	  // found matching response
	  int delay = (t->rsptime - req_reorder.top()->reqtime)/1000000;
#if 0	  
	  printf("DONE %s %s %u %lld %lld %lld %d %s\n",
		 ipstring(req_reorder.top()->sourceip).c_str(),
		 ipstring(req_reorder.top()->destip).c_str(),
		 req_reorder.top()->transactionid,
		 req_reorder.top()->reqtime,
		 t->rsptime,
		 reqtime.val(),
		 delay,
		 req_reorder.top()->operation.c_str());
#endif  
	  Complete.add(delay);

	  // remove response from pending hashtable
	  pending.remove(*t);
	}

	// remove request from priority queue
	delete req_reorder.top();
	req_reorder.pop();

	if(req_reorder.empty() == false)
	  age=(reqtime.val()-req_reorder.top()->reqtime)/1000000000;
	else
	  age=0;
      }
    }

    return e;
  }

  virtual void printResult() {

    // check for completed transactions
    while(req_reorder.empty() == false &&
	  pending.size() > 0) {

      // check for a matching response
      rspData tmp;
      tmp.destip = req_reorder.top()->sourceip;
      tmp.transactionid = req_reorder.top()->transactionid;

      rspData *t = pending.lookup(tmp);

      if(t == NULL) {
	// no matching response found; drop request
#if 0
	printf("NO_RSP %s %s %x %lld\n",
	       ipstring(req_reorder.top()->sourceip).c_str(),
	       ipstring(req_reorder.top()->destip).c_str(),
	       req_reorder.top()->transactionid,
	       req_reorder.top()->reqtime);
#endif
	NoResponse.add(1);
	  } else {
	    // found matching response
	    int delay = (t->rsptime - req_reorder.top()->reqtime)/1000000;
#if 0
	    printf("DONE %s %s %u %lld %lld %d %s\n",
		   ipstring(req_reorder.top()->sourceip).c_str(),
		   ipstring(req_reorder.top()->destip).c_str(),
		   req_reorder.top()->transactionid,
		   req_reorder.top()->reqtime,
		   t->rsptime,
		   delay,
		   req_reorder.top()->operation.c_str());
#endif
	    Complete.add(delay);

	    // remove response from pending hashtable
	    pending.remove(*t);
	  }

      // remove request from priority queue
      delete req_reorder.top();
      req_reorder.pop();
    }

    if(pending.size() > 0) {

      for(HashTable<rspData, rspHash, rspEqual>::iterator i =
	    pending.begin();
	  i != pending.end();
	  ++i)
	{
#if 0
	  printf("NO_REQ %s %s %u %lld %s\n",
		 ipstring(i->destip).c_str(),
		 ipstring(i->sourceip).c_str(),
		 i->transactionid,
		 i->rsptime,
		 i->operation.c_str());
#endif
	  NoRequest.add(1);
	}

    }

    printf("Begin-%s\n",__PRETTY_FUNCTION__);
    printf(" Complete Transactions: %.0f\n", Complete.count()/1.0);
    printf("       Missing Request: %.0f\n", NoRequest.count()/1.0);
    printf("      Missing Response: %.0f\n", NoResponse.count()/1.0);
    printf("Retransmitted Response: %.0f\n", RetransmitRsp.count()/1.0);
    printf("\n");
    printf(" Minimum Response Time: %.0f\n", Complete.min());
    printf(" Maximum Response Time: %.0f\n", Complete.max());
    printf("    Mean Response Time: %.0f\n", Complete.mean());

    printf("End-%s\n",__PRETTY_FUNCTION__);
    }
};

NFSDSModule *
NFSDSAnalysisMod::newTransactions(DataSeriesModule &prev)
{
    return new Transactions(prev);
}


class OutstandingRequests : public NFSDSModule {
public:

  static const unsigned buffer_delay = 1;  // seconds

    OutstandingRequests(DataSeriesModule &_source, int _latency_offset) 
      : latency_offset(_latency_offset),
        source(_source), 
        s(ExtentSeries::typeExact),
	reqtime(s,"packet-at"),
	sourceip(s,"source"),destip(s,"dest"),	  
	is_udp(s,"is-udp"),
	is_request(s,"is-request"),
	transaction_id(s, "transaction-id"),
	op_id(s,"op-id",Field::flag_nullable),
	operation(s,"operation")
    {}

  virtual ~OutstandingRequests() { }

  int latency_offset;
  DataSeriesModule &source;
  ExtentSeries s;
  Int64Field reqtime;
  Int32Field sourceip, destip;
  BoolField is_udp;
  BoolField is_request;
  Int32Field transaction_id;
  ByteField op_id;
  Variable32Field operation;
  Stats Complete, NoRequest, NoResponse, RetransmitRsp;

  // data structure for prioritizing requests
  struct reqinfo { 
    ExtentType::int64 reqtime;
    ExtentType::int32 sourceip, destip;
    ExtentType::int32 transactionid;
    string operation;
  };

  struct reqinfo_geq { 
    bool operator()(const reqinfo *a, const reqinfo *b) {
      return a->reqtime > b->reqtime;
    }
  };
  PriorityQueue<reqinfo *,reqinfo_geq> req_reorder;

  // data structure to keep responses until we have sorted requests
  struct rspData {
    ExtentType::int64 rsptime;
    ExtentType::int32 sourceip, destip;
    ExtentType::int32 transactionid;
    string operation;
  };
  
  class rspHash {
  public: unsigned int operator()(const rspData &r) {
    return r.transactionid ^ r.destip;
  }};
  
  class rspEqual {
  public: bool operator()(const rspData &r1, const rspData &r2) {
    return r1.transactionid == r2.transactionid && r1.destip == r2.destip;
  }};

  HashTable<rspData, rspHash, rspEqual> pending;

  // data structure for prioritizing requests
  struct transinfo { 
    ExtentType::int64 end;
    ExtentType::int32 clientip, serverip;
  };

  struct transinfo_geq { 
    bool operator()(const transinfo *a, const transinfo *b) {
      return a->end > b->end;
    }
  };
  
  PriorityQueue<transinfo *,transinfo_geq> trans_reorder;

  // data structure for storing transactions by machine
  struct transData {
    ExtentType::int32 ip_addr;
    unsigned int count;
    unsigned int total;
  };

  class transHash {
  public: unsigned int operator()(const transData &t) {
    return t.ip_addr;
  }};
  
  class transEqual {
  public: bool operator()(const transData &t1, const transData &t2) {
    return t1.ip_addr == t2.ip_addr;
  }};

  HashTable<transData, transHash, transEqual> Clients;
  HashTable<transData, transHash, transEqual> Servers;

  virtual Extent *getExtent() {
    Extent *e = source.getExtent();
    if (e == NULL) 
      return NULL;
    if (e->type.getName() != "NFS trace: common")
      return e;

    for(s.setExtent(e);s.pos.morerecords();++s.pos) {
      if (op_id.isNull())
	continue;

      if (is_request.val()) {
	// add request to priority queue

	reqinfo *tmp = new reqinfo;
	tmp->reqtime = reqtime.val();
	tmp->sourceip = sourceip.val();
	tmp->destip = destip.val();
	tmp->transactionid = transaction_id.val();
	tmp->operation = operation.stringval();
	
	req_reorder.push(tmp);
      }
      else { 
	// response
	rspData dummy;
	dummy.rsptime = reqtime.val();
	dummy.sourceip = sourceip.val();
	dummy.destip = destip.val();
	dummy.transactionid = transaction_id.val();
	dummy.operation = operation.stringval();
	
	rspData *t = pending.lookup(dummy);
	if (t == NULL) {
	  // add to hashtable of pending responses
	  t = pending.add(dummy);
	} else {
	  // response is a retransmit
	  RetransmitRsp.add(1);
	} 
      }

      // check for completed transactions
      Extent::int64 age;
      if(req_reorder.empty() == false)
	age=(reqtime.val()-req_reorder.top()->reqtime)/1000000000;
      else
	age=0;

      while(req_reorder.empty() == false &&
	    s.pos.morerecords() &&
	    (age >= buffer_delay)) {

	// check for a matching response
	rspData tmp;
	tmp.destip = req_reorder.top()->sourceip;
	tmp.transactionid = req_reorder.top()->transactionid;

	rspData *t = pending.lookup(tmp);
	if(t == NULL) {
	  // no matching response found; drop request
	  NoResponse.add(1);
#if 0
	  printf("NO_RSP %s %s %x %lld %lld %lld %lld\n",
		 ipstring(req_reorder.top()->sourceip).c_str(),
		 ipstring(req_reorder.top()->destip).c_str(),
		 req_reorder.top()->transactionid,
		 req_reorder.top()->reqtime,
		 reqtime.val(),
		 reqtime.val() - req_reorder.top()->reqtime,
		 age);	  
#endif
	} else {
	  // found matching response
	  int delay = (t->rsptime - req_reorder.top()->reqtime)/1000000;
#if 0	  
	  printf("DONE %s %s %u %lld %lld %lld %d %s\n",
		 ipstring(req_reorder.top()->sourceip).c_str(),
		 ipstring(req_reorder.top()->destip).c_str(),
		 req_reorder.top()->transactionid,
		 req_reorder.top()->reqtime,
		 t->rsptime,
		 reqtime.val(),
		 delay,
		 req_reorder.top()->operation.c_str());
#endif  
	  Complete.add(delay);

	    // remove completed transactions from queue
	    while(trans_reorder.empty() == false &&
		  trans_reorder.top()->end < 
		  (req_reorder.top()->reqtime/1000000)) {
	      
	      // update count on appropriate hosts
	      transData cli;
	      cli.ip_addr = trans_reorder.top()->clientip;
	      transData *cd = Clients.lookup(cli);
	      if(cd == NULL) {
		printf("CLIENT_ERROR\n");
	      } else {
		cd->count--;
#if 0
		printf("CLI %lld %x %u\n", 
		       trans_reorder.top()->end/1000,
		       cd->ip_addr,
		       cd->count);
#endif
	      }

	      // update count on appropriate server
	      transData svr;
	      svr.ip_addr = trans_reorder.top()->serverip;
	      transData *sd = Servers.lookup(svr);
	      if(sd == NULL) {
		printf("SERVER_ERROR\n");
	      } else {
		sd->count--;
#if 0
		printf("SVR %lld %x %u\n", 
		       trans_reorder.top()->end/1000,
		       sd->ip_addr,
		       sd->count);
#endif
	      }

	      // remove transaction
	      delete trans_reorder.top();
	      trans_reorder.pop();
	    }

	  // add client to hash table
	  transData cli;
	  cli.ip_addr = req_reorder.top()->sourceip;
	  transData *cr = Clients.lookup(cli);

	  if(cr == NULL) {
	    // new client
	    // printf("NEW_CLIENT %x\n", cli.ip_addr);
	    cli.count = 1;
	    cli.total = 1;
	    cr = Clients.add(cli);
	  } else {
	    cr->count++;
	    cr->total++;
	  }

	  if((cr->total % 100) == 0)
	    printf("CLI %lld %x %u\n", 
		   req_reorder.top()->reqtime/1000000000,
		   cr->ip_addr,
		   cr->count);

	  // add server to hash table
	  transData svr;
	  svr.ip_addr = req_reorder.top()->destip;
	  transData *sr = Servers.lookup(svr);

	  if(sr == NULL) {
	    // new server
	    // printf("NEW_SERVER %x\n", svr.ip_addr);
	    svr.count = 1;
	    svr.total = 1;
	    sr = Servers.add(svr);
	  } else {
	    sr->count++;
	    sr->total++;
	  }
	  
	  if((sr->total % 100)==0)
	    printf("SVR %lld %x %u\n", 
		   req_reorder.top()->reqtime/1000000000,
		   sr->ip_addr,
		   sr->count);

	  // queue transaction
	  transinfo *newt = new transinfo;
	  newt->end = (t->rsptime/1000000 + latency_offset);
	  newt->clientip = req_reorder.top()->sourceip;
	  newt->serverip = req_reorder.top()->destip;
	  trans_reorder.push(newt);

	  // remove response from pending hashtable
	  pending.remove(*t);
	}

	// remove request from priority queue
	delete req_reorder.top();
	req_reorder.pop();

	if(req_reorder.empty() == false)
	  age=(reqtime.val()-req_reorder.top()->reqtime)/1000000000;
	else
	  age=0;
      }
    }

    return e;
  }

  virtual void printResult() {

    // check for completed transactions
    while(req_reorder.empty() == false &&
	  pending.size() > 0) {

      // check for a matching response
      rspData tmp;
      tmp.destip = req_reorder.top()->sourceip;
      tmp.transactionid = req_reorder.top()->transactionid;

      rspData *t = pending.lookup(tmp);

      if(t == NULL) {
	// no matching response found; drop request
#if 0
	printf("NO_RSP %s %s %x %lld\n",
	       ipstring(req_reorder.top()->sourceip).c_str(),
	       ipstring(req_reorder.top()->destip).c_str(),
	       req_reorder.top()->transactionid,
	       req_reorder.top()->reqtime);
#endif
	NoResponse.add(1);
	  } else {
	    // found matching response
	    int delay = (t->rsptime - req_reorder.top()->reqtime)/1000000;
#if 0
	    printf("DONE %s %s %u %lld %lld %d %s\n",
		   ipstring(req_reorder.top()->sourceip).c_str(),
		   ipstring(req_reorder.top()->destip).c_str(),
		   req_reorder.top()->transactionid,
		   req_reorder.top()->reqtime,
		   t->rsptime,
		   delay,
		   req_reorder.top()->operation.c_str());
#endif
	    Complete.add(delay);


	    
	    // remove response from pending hashtable
	    pending.remove(*t);
	  }

      // remove request from priority queue
      delete req_reorder.top();
      req_reorder.pop();
    }

    // remove completed transactions from queue
    while(trans_reorder.empty() == false) {

      // update count on appropriate hosts
      transData cli;
      cli.ip_addr = trans_reorder.top()->clientip;
      transData *cd = Clients.lookup(cli);
      if(cd == NULL) {
	printf("CLIENT_ERROR\n");
      } else {
	cd->count--;
	if((cd->total % 100)==0)
	  printf("CLI %lld %x %u\n", 
		 trans_reorder.top()->end/1000,
		 cd->ip_addr,
		 cd->count);
      }
      
      // update count on appropriate server
      transData svr;
      svr.ip_addr = trans_reorder.top()->serverip;
      transData *sd = Servers.lookup(svr);
      if(sd == NULL) {
	printf("SERVER_ERROR\n");
      } else {
	sd->count--;
	if((sd->total % 100)==0)
	  printf("SVR %lld %x %u\n", 
		 trans_reorder.top()->end/1000,
		 sd->ip_addr,
		 sd->count);
      }
      
      // remove transaction
      delete trans_reorder.top();
      trans_reorder.pop();
    }

    if(pending.size() > 0) {

      for(HashTable<rspData, rspHash, rspEqual>::iterator i =
	    pending.begin();
	  i != pending.end();
	  ++i)
	{
#if 0
	  printf("NO_REQ %s %s %u %lld %s\n",
		 ipstring(i->destip).c_str(),
		 ipstring(i->sourceip).c_str(),
		 i->transactionid,
		 i->rsptime,
		 i->operation.c_str());
#endif
	  NoRequest.add(1);
	}

    }

    printf("Begin-%s\n",__PRETTY_FUNCTION__);
    printf(" Complete Transactions: %.0f\n", Complete.count()/1.0);
    printf("       Missing Request: %.0f\n", NoRequest.count()/1.0);
    printf("      Missing Response: %.0f\n", NoResponse.count()/1.0);
    printf("Retransmitted Response: %.0f\n", RetransmitRsp.count()/1.0);
    printf("\n");
    printf(" Minimum Response Time: %.0f\n", Complete.min());
    printf(" Maximum Response Time: %.0f\n", Complete.max());
    printf("    Mean Response Time: %.0f\n", Complete.mean());

    printf("End-%s\n",__PRETTY_FUNCTION__);
    }
};


NFSDSModule *
NFSDSAnalysisMod::newOutstandingRequests(DataSeriesModule &prev,
					 int latency_offset)
{
    return new OutstandingRequests(prev, latency_offset);
}


