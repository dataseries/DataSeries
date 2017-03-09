// -*-C++-*-
/*
  (c) Copyright 2008 Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file 

    Convert custom dataseries format back into the custom World Cup
    format.  http://ita.ee.lbl.gov/html/contrib/WorldCup.html
*/

/*
=pod

=head1 NAME

ds2wcweb - convert from dataseries to the custom 1998 world cup trace format

=head1 SYNOPSIS

% ds2wcweb [ds-common-args] <input-ds-paths...> | gzip -9v >trace.out

=head1 DESCRIPTION

ds2wcweb converts the Log::Web::WorldCup::Custom dataseries traces
back to the 1998 world cup custom traces format.  The world cup traces
are a very large, publically available set of web traces.  They have
been published in a special binary record format that was designed to
reduce space usage.  This converter converts from a dataseries version
to that record format.

=head1 EXAMPLES

% gz | wcweb2ds --compress-bz2 - wc_day46_3.ds
% wcweb2ds --compress-gz --extent-size=1000000 wc_day80_1 wc_day80_1.ds

=head1 SEE ALSO

wcweb2ds(1), DataSeries(5), DSCommonArgs(1)

=head1 AUTHOR/CONTACT

Eric Anderson <software@cello.hpl.hp.com>
=cut
*/

#include <arpa/inet.h>
 
#include <DataSeries/PrefetchBufferModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

struct record {
    uint32_t timestamp, clientID, objectID, size;
    uint8_t method, status, type, server;
};

class WorldCupDSToCustom : public RowAnalysisModule {
  public:
    WorldCupDSToCustom(DataSeriesModule &source)
    : RowAnalysisModule(source),
      timestamp(series, "timestamp"),
      clientID(series, "clientID"),
      objectID(series, "objectID"),
      size(series, "size"),
      method(series, "method"),
      status(series, "http_version_and_status"),
      type(series, "type"),
      server(series, "server")
    { }

    virtual ~WorldCupDSToCustom() { }

    virtual void processRow() {
        record buf;
        buf.timestamp = htonl(timestamp.val());
        buf.clientID = htonl(clientID.val());
        buf.objectID = htonl(objectID.val());
        buf.size = htonl(size.val());
        buf.method = method.val();
        buf.status = status.val();
        buf.type = type.val();
        buf.server = server.val();
        
        size_t amt = fwrite(&buf, 1, sizeof(record), stdout);
        SINVARIANT(amt == sizeof(record) && !ferror(stdout));
    }

    virtual void printResult() {
        CHECKED(fclose(stdout) == 0, "close failed");
    }

  private:
    Int32Field timestamp;
    Int32Field clientID;
    Int32Field objectID;
    Int32Field size;
    ByteField method;
    ByteField status;
    ByteField type;
    ByteField server;
};

int main(int argc, char *argv[]) {
    TypeIndexModule *source
            = new TypeIndexModule("Log::Web::WorldCup::Custom");

    INVARIANT(argc >= 2 && strcmp(argv[1], "-h") != 0,
              boost::format("Usage: %s <file...> | gzip >output-path\n") % argv[0]);

    for (int i = 1; i < argc; ++i) {
        source->addSource(argv[i]);
    }

    SequenceModule seq(source);
    
    seq.addModule(new WorldCupDSToCustom(seq.tail()));
    
    seq.getAndDeleteShared();
    RowAnalysisModule::printAllResults(seq);
    return 0;
}

