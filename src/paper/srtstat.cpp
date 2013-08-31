// -*-C++-*-
/*
  (c) Copyright 2007, Hewlett-Packard Development Company, LP

  See the file named COPYING for license details
*/

/** @file
    SRT-specfic statistic calculation, for purpose of the DataSeries paper
*/

/* 
   Procesing of harp/old-0; repacked to groups of 10 files.
   Times in user system wall, RHEL4-64bit 2.4GhZ 2x2core Opteron 2216HE
   One run to prefetch data; 89,416,000 rows

   % du -k
   1439756 ./lzo-96k
   1467080 ./lzo-64k
   1290668 ./gz-96k

   -d 2 gz-96k: 37.34 2.05 10.009 ; 36.93 2.42 10.034 ; 37.21 2.07 9.997
   -d 2 lzo-64k: 24.86 2.43 6.965 ; 24.87 2.33 6.944 ; 24.96 2.38 6.976
   -d 2 lzo-96k: 24.37 2.27 6.809 ; 24.57 2.17 6.814 ; 24.52 2.11 6.818
   -d 2 lzo-128k 24.37 2.30 6.805 ; 24.66 2.34 6.908 ; 24.41 2.20 6.813

   -c 2 lzo-96k: 33.69 2.97 9.572 ; 33.74 2.94 9.509 ; 33.68 3.08 9.530
   -b 2 lzo-96k: 33.01 2.92 9.233 ; 33.15 3.07 9.333 ; 32.45 2.80 8.979
   -a 2 lzo-96k: 33.32 2.89 9.313 ; 33.18 2.90 9.262 ; 33.34 2.91 9.316
   -j 2 lzo-96k: 32.54 2.87 9.08 ; 32.43 2.68 8.95 ; 32.43 2.66 8.96


   // ~4us/access
   -e 2 lzo-96k: 33.76 2.96 9.66 ; 33.09 3.09 9.36 ; 33.14 3.18 9.38
   -e 5 lzo-96k: 38.97 3.28 15.06 ; 39.11 3.34 15.09 ; 38.87 3.19 15.12
   -e 9 lzo-96k: 44.39 3.56 20.56 ; 46.22 3.31 22.16 ; 45.75 3.63 22.23

   // 2.75us/access
   -f 2 lzo-96k: 32.00 2.40 8.74 ; 32.35 2.31 8.80 ; 32.33 2.40 8.82
   -f 9 lzo-96k: 40.98 3.31 17.20 ; 40.90 3.56 17.20 ; 40.96 3.27 17.20

   // 7.1us/access
   -g 2 lzo-96k: 33.81 3.09 9.81 ; 33.64 3.22 9.95 ; 32.60 3.02 9.07
   -g 9 lzo-96k: 56.22 3.03 32.51 ; 55.35 2.67 31.51 ; 55.81 2.66 31.98

   // ~0.3ns/access
   -i 2 lzo-96k: 31.38 2.51 8.61 ; 31.29 2.51 8.58 ; 31.55 2.35 8.61
   -i 9 lzo-96k: 32.38 2.39 8.84 ; 32.44 2.57 8.93 ; 32.52 2.64 8.99

   dsstatgroupby HP-UX basic 'leave_driver - enter_driver' group by logical_volume_number from lzo-96k/hourly.00*
   38.58 2.45 14.50 ; 38.63 2.49 14.59 ; 38.65 2.42 14.58
   

   Debian etch-32bit

   -d 2 gz-96k: 46.79 3.36 12.60 ; 46.70 3.00 13.08 ; 46.67 3.16 12.67
   -d 2 lzo-64k: 22.03 3.56 6.50 ; 22.05 3.22 6.95 ; 22.16 3.32 6.44
   -d 2 lzo-96k: 22.21 2.93 6.33 ; 22.19 2.73 6.30 ; 21.95 2.87 6.89
   -d 2 lzo-128k: 21.92 3.21 6.34 ; 21.98 3.36 6.40 ; 22.04 3.06 6.32

   -c 2 lzo-96k: 33.76 3.66 12.41 ; 34.00 3.53 12.66 ; 33.49 3.75 12.16
   -b 2 lzo-96k: 31.97 3.89 9.75 ; 31.86 3.90 10.10 ; 31.38 4.27 9.39
   -a 2 lzo-96k: 31.82 3.97 9.42 ; 31.41 3.67 9.76; 32.18 3.73 10.17

   // ~0.8ns/extra access
   -e 1 lzo-96k: 22.50 2.98 6.99 ; 22.71 2.96 6.84 ; 22.68 2.90 6.53
   -e 2 lzo-96k: 30.65 3.07 8.63 ; 30.59 3.21 9.06 ; 29.71 3.15 8.29
   -e 3 lzo-96k: 31.03 3.89 9.10 ; 30.77 3.70 9.10 ; 30.65 3.21 9.05
   -e 5 lzo-96k: 31.21 3.58 9.41 ; 31.77 4.02 9.42 ; 31.77 3.68 9.31
   -e 9 lzo-96k: 32.96 3.68 11.30 ; 32.95 3.54 11.06 ; 33.53 3.56 11.03

   // ~5.4ns/extra access -- opterons may have branch prediction issues with lots of
   // branches in a 16 byte window
   -f 2 lzo-96k: 31.41 3.36 9.19 ; 31.69 3.10 9.27 ; 31.95 3.82 9.68
   -f 3 lzo-96k: 32.17 3.02 11.95 ; 32.28 3.32 11.81 ; 31.99 3.25 11.33
   -f 5 lzo-96k: 37.17 3.29 16.76 ; 37.09 3.36 16.35 ; 36.99 3.40 16.02
   -f 9 lzo-96k: 48.69 3.06 27.49 ; 48.58 2.86 28.28 ; 48.69 3.09 27.58

   // ~14.5ns/extra access
   -g 2 lzo-96k: 34.18 3.32 12.14 ; 33.15 3.86 10.83 ; 33.25 3.70 10.86
   -g 3 lzo-96k: 39.73 3.27 19.08 ; 40.54 3.58 19.12 ; 39.83 3.31 18.68
   -g 5 lzo-96k: 52.84 2.68 31.63 ; 53.69 2.70 32.48 ; 54.40 3.03 33.04
   -g 9 lzo-96k: 78.53 2.42 57.02 ; 78.53 2.41 58.34 ; 78.26 2.73 56.94

   // ~1.0ns/access
   -i 2 lzo-96k: 30.87 3.17 8.72 ; 30.53 3.15 8.49 ; 30.51 2.99 8.47
   -i 9 lzo-96k: 33.21 3.82 11.35 ; 32.98 3.68 11.35 ; 32.58 3.82 10.83


   perl -e 'use Time::HiRes "time"; use BSD::Resource; my $start = time; system(@ARGV); $end = time; $rusage = getrusage(RUSAGE_CHILDREN); printf "%.2f %.2f %.2f\n", $rusage->utime, $rusage->stime, $end - $start;'
*/

#include <string>

#include <boost/format.hpp>

#include <Lintel/Stats.hpp>
#include <Lintel/HashMap.hpp>
#include <Lintel/AssertBoost.hpp>

#include <DataSeries/GeneralField.hpp>
#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

using namespace std;
using dataseries::TFixedField;
using boost::format;

struct HashMap_hashintfast {
    unsigned operator()(const int _a) const {
        return _a;
    }
};

class LatencyGroupByIntBasic : public RowAnalysisModule {
  public:
    LatencyGroupByIntBasic(DataSeriesModule &source, const string &_start_field, 
                           const string &_end_field, const string &_groupby_field)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field)
    {
    }

    typedef HashMap<ExtentType::int32, Stats *, HashMap_hashintfast> mytableT;

    virtual ~LatencyGroupByIntBasic() { }

    virtual void prepareForProcessing() {
        
    }

    virtual void processRow() {
        Stats *stat = mystats[groupby.val()];
        if (stat == NULL) {
            stat = new Stats();
            mystats[groupby.val()] = stat;
        }
        stat->add(end_time.valRaw() - start_time.valRaw());
    }

    double toSec(double v) {
        Int64TimeField::Raw raw = static_cast<int64_t>(round(v));
        return start_time.rawToDoubleSeconds(raw);
    }

    virtual void printResult() {
        cout << boost::format("%s, count(*), mean(%s - %s), stddev, min, max # basic field")
                % groupby_field % end_field % start_field << endl;
        for (mytableT::iterator i = mystats.begin(); 
            i != mystats.end(); ++i) {
            cout << boost::format("%d, %d, %.6g, %.6g, %.6g, %.6g\n") 
                    % i->first % i->second->count() % toSec(i->second->mean()) 
                    % toSec(i->second->stddev()) % toSec(i->second->min()) % toSec(i->second->max());
        }
    }

    mytableT mystats;
    string start_field, end_field, groupby_field;
    Int64TimeField start_time, end_time;
    Int32Field groupby;
};

class LatencyGroupByIntFixed : public RowAnalysisModule {
  public:
    LatencyGroupByIntFixed(DataSeriesModule &source, const string &_start_field, 
                           const string &_end_field, const string &_groupby_field)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field)
    {
    }

    typedef HashMap<ExtentType::int32, Stats *, HashMap_hashintfast> mytableT;

    virtual ~LatencyGroupByIntFixed() { }

    virtual void prepareForProcessing() {
        
    }

    virtual void processRow() {
        Stats *stat = mystats[groupby.val()];
        if (stat == NULL) {
            stat = new Stats();
            mystats[groupby.val()] = stat;
        }
        stat->add(end_time.val() - start_time.val());
    }

    double toSec(double v) {
        Int64TimeField::Raw raw = static_cast<int64_t>(round(v));
        // Ought to check units here
        return Clock::TfracToDouble(raw);
    }

    virtual void printResult() {
        cout << boost::format("%s, count(*), mean(%s - %s), stddev, min, max # fixed field")
                % groupby_field % end_field % start_field << endl;
        for (mytableT::iterator i = mystats.begin(); 
            i != mystats.end(); ++i) {
            cout << boost::format("%d, %d, %.6g, %.6g, %.6g, %.6g\n") 
                    % i->first % i->second->count() % toSec(i->second->mean()) 
                    % toSec(i->second->stddev()) % toSec(i->second->min()) % toSec(i->second->max());
        }
    }

    mytableT mystats;
    string start_field, end_field, groupby_field;
    TFixedField<int64_t> start_time, end_time;
    TFixedField<int32_t> groupby;
};

class LatencyGroupByIntGeneral : public RowAnalysisModule {
  public:
    LatencyGroupByIntGeneral(DataSeriesModule &source, const string &_start_field,
                             const string &_end_field, const string &_groupby_field)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field)
    {
    }

    typedef HashMap<ExtentType::int32, Stats *, HashMap_hashintfast> mytableT;

    virtual ~LatencyGroupByIntGeneral() { 
        delete start_time;
        delete end_time;
        delete groupby;
    }

    virtual void prepareForProcessing() {
        start_time = GeneralField::create(series, start_field);
        end_time = GeneralField::create(series, end_field);
        groupby = GeneralField::create(series, groupby_field);
    }

    virtual void processRow() {
        Stats *stat = mystats[static_cast<int32_t>(groupby->valDouble())];
        if (stat == NULL) {
            stat = new Stats();
            mystats[static_cast<int32_t>(groupby->valDouble())] = stat;
        }
        stat->add(end_time->valDouble() - start_time->valDouble());
    }

    virtual void printResult() {
        cout << boost::format("%s, count(*), mean(%s - %s), stddev, min, max # fixed field")
                % groupby_field % end_field % start_field << endl;
        for (mytableT::iterator i = mystats.begin(); 
            i != mystats.end(); ++i) {
            cout << boost::format("%d, %d, %.6g, %.6g, %.6g, %.6g") 
                    % i->first % i->second->count() % i->second->mean() % i->second->stddev()
                    % i->second->min() % i->second->max() 
                 << endl;
        }
    }

    mytableT mystats;
    string start_field, end_field, groupby_field;
    GeneralField *start_time, *end_time;
    GeneralField *groupby;
};

class LatencyGroupByNoop : public RowAnalysisModule {
  public:
    LatencyGroupByNoop(DataSeriesModule &source, const string &_start_field, 
                       const string &_end_field, const string &_groupby_field)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field)
    { }

    virtual void processRow() {
    }

    virtual void printResult() {
    }

    string start_field, end_field, groupby_field;
    TFixedField<int64_t> start_time, end_time;
    TFixedField<int32_t> groupby;
};

class LatencyGroupByAccessFixed : public RowAnalysisModule {
  public:
    LatencyGroupByAccessFixed(DataSeriesModule &source, const string &_start_field, 
                              const string &_end_field, const string &_groupby_field, 
                              uint32_t _reps)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field), reps(_reps), sum(0), row_count(0)
    { }

    virtual void processRow() {
        for (uint32_t i = 0; i < reps; ++i) {
            sum += start_time.val();
            sum += end_time.val();
            sum += groupby.val();
            sum += start_time.val();
            sum += end_time.val();
        }
        ++row_count;
    }

    virtual void printResult() {
        cout << boost::format("%d rows; sum %d\n") % row_count % sum;
    }

    string start_field, end_field, groupby_field;
    TFixedField<int64_t> start_time, end_time;
    TFixedField<int32_t> groupby;
    uint32_t reps;
    int64_t sum;
    uint64_t row_count;
};

// Special case to test compiler handling templates.

class NNInt64Field : public FixedField {
  public:
    NNInt64Field(ExtentSeries &_dataseries, const std::string &field)
            : FixedField(_dataseries, field, ExtentType::ft_int64, 0) 
    { 
        _dataseries.addField(*this);
    }

    virtual ~NNInt64Field() { };

    int64_t val() const { 
        return *reinterpret_cast<int64_t *>(rowPos() + offset);
    }
};

class NNInt32Field : public FixedField {
  public:
    NNInt32Field(ExtentSeries &_dataseries, const std::string &field)
            : FixedField(_dataseries, field, ExtentType::ft_int32, 0) 
    { 
        _dataseries.addField(*this);
    }

    virtual ~NNInt32Field() { };

    int32_t val() const { 
        return *reinterpret_cast<int32_t *>(rowPos() + offset);
    }
};

class LatencyGroupByIntNN : public RowAnalysisModule {
  public:
    LatencyGroupByIntNN(DataSeriesModule &source, const string &_start_field, 
                        const string &_end_field, const string &_groupby_field)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field)
    { }

    typedef HashMap<ExtentType::int32, Stats *, HashMap_hashintfast> mytableT;

    virtual void processRow() {
        Stats *stat = mystats[groupby.val()];
        if (stat == NULL) {
            stat = new Stats();
            mystats[groupby.val()] = stat;
        }
        stat->add(end_time.val() - start_time.val());
    }

    double toSec(double v) {
        Int64TimeField::Raw raw = static_cast<int64_t>(round(v));
        // Ought to check units here
        return Clock::TfracToDouble(raw);
    }

    virtual void printResult() {
        cout << boost::format("%s, count(*), mean(%s - %s), stddev, min, max # fixed field")
                % groupby_field % end_field % start_field << endl;
        for (mytableT::iterator i = mystats.begin(); 
            i != mystats.end(); ++i) {
            cout << boost::format("%d, %d, %.6g, %.6g, %.6g, %.6g\n") 
                    % i->first % i->second->count() % toSec(i->second->mean()) 
                    % toSec(i->second->stddev()) % toSec(i->second->min()) % toSec(i->second->max());
        }
    }

    mytableT mystats;
    string start_field, end_field, groupby_field;
    NNInt64Field start_time, end_time;
    NNInt32Field groupby;
};

class LatencyGroupByAccessNN : public RowAnalysisModule {
  public:
    LatencyGroupByAccessNN(DataSeriesModule &source, const string &_start_field, 
                           const string &_end_field, const string &_groupby_field, 
                           uint32_t _reps)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field), reps(_reps), sum(0), row_count(0)
    { }

    virtual void processRow() {
        for (uint32_t i = 0; i < reps; ++i) {
            sum += start_time.val();
            sum += end_time.val();
            sum += groupby.val();
            sum += start_time.val();
            sum += end_time.val();
        }
        ++row_count;
    }

    virtual void printResult() {
        cout << boost::format("%d rows; sum %d\n") % row_count % sum;
    }

    string start_field, end_field, groupby_field;
    NNInt64Field start_time, end_time;
    NNInt32Field groupby;
    uint32_t reps;
    int64_t sum;
    uint64_t row_count;
};

class LatencyGroupByAccessBasic : public RowAnalysisModule {
  public:
    LatencyGroupByAccessBasic(DataSeriesModule &source, const string &_start_field, 
                              const string &_end_field, const string &_groupby_field, 
                              uint32_t _reps)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              start_time(series, start_field), 
              end_time(series, end_field), 
              groupby(series, groupby_field), reps(_reps), sum(0), row_count(0)
    { }

    virtual void processRow() {
        for (uint32_t i = 0; i < reps; ++i) {
            sum += start_time.val();
            sum += end_time.val();
            sum += groupby.val();
            sum += start_time.val();
            sum += end_time.val();
        }
        ++row_count;
    }

    virtual void printResult() {
        cout << boost::format("%d rows; sum %d\n") % row_count % sum;
    }

    string start_field, end_field, groupby_field;
    Int64Field start_time, end_time;
    Int32Field groupby;
    uint32_t reps;
    int64_t sum;
    uint64_t row_count;
};

class LatencyGroupByAccessGeneral : public RowAnalysisModule {
  public:
    LatencyGroupByAccessGeneral(DataSeriesModule &source, const string &_start_field, 
                                const string &_end_field, const string &_groupby_field, 
                                uint32_t _reps)
            : RowAnalysisModule(source), 
              start_field(_start_field),
              end_field(_end_field),
              groupby_field(_groupby_field),
              reps(_reps), sum(0), row_count(0)
    { }

    virtual ~LatencyGroupByAccessGeneral() { 
        delete start_time;
        delete end_time;
        delete groupby;
    }

    virtual void prepareForProcessing() {
        start_time = GeneralField::create(series, start_field);
        end_time = GeneralField::create(series, end_field);
        groupby = GeneralField::create(series, groupby_field);
    }

    virtual void processRow() {
        for (uint32_t i = 0; i < reps; ++i) {
            sum += start_time->valDouble();
            sum += end_time->valDouble();
            sum += groupby->valDouble();
            sum += start_time->valDouble();
            sum += end_time->valDouble();
        }
        ++row_count;
    }

    virtual void printResult() {
        cout << boost::format("%d rows\n") % row_count;
    }

    string start_field, end_field, groupby_field;
    GeneralField *start_time, *end_time;
    GeneralField *groupby;
    uint32_t reps;
    double sum;
    uint64_t row_count;
};

int main(int argc, char *argv[]) {
    TypeIndexModule *source = new TypeIndexModule("Trace::BlockIO::HP-UX");

    SequenceModule seq(source);
    while (1) {
        int opt = getopt(argc, argv, "a:b:c:d:e:f:g:i:j:");
        if (opt == -1) break;
        int variant = stringToInteger<int32_t>(optarg);
        string start_field, end_field, groupby_field;
        SINVARIANT(variant >= 1 && variant <= 9);
        if (variant <= 6) {
            start_field = "enter_driver";
        } else {
            start_field = "leave_driver";
        }

        if (variant <= 3) {
            end_field = "leave_driver";
        } else {
            end_field = "return_to_driver";
        }

        if ((variant % 3) == 1) {
            groupby_field = "pid";
        } else if ((variant % 3) == 2) {
            groupby_field = "logical_volume_number";
        } else {
            groupby_field = "bytes";
        }

        switch(opt) 
        {
            case 'a':
                seq.addModule(new LatencyGroupByIntBasic(seq.tail(), start_field, 
                                                         end_field, groupby_field));
                break;
            case 'b':
                seq.addModule(new LatencyGroupByIntFixed(seq.tail(), start_field, 
                                                         end_field, groupby_field));
                break;
            case 'c':
                seq.addModule(new LatencyGroupByIntGeneral(seq.tail(), start_field, 
                                                           end_field, groupby_field));
                break;
            case 'd':
                seq.addModule(new LatencyGroupByNoop(seq.tail(), start_field, 
                                                     end_field, groupby_field));
                break;
            case 'e': seq.addModule(new LatencyGroupByAccessFixed
                                    (seq.tail(), "enter_driver", "leave_driver",
                                     "logical_volume_number", variant-1));
                break;
            case 'f': seq.addModule(new LatencyGroupByAccessBasic
                                    (seq.tail(), "enter_driver", "leave_driver",
                                     "logical_volume_number", variant-1));
                break;
            case 'g': seq.addModule(new LatencyGroupByAccessGeneral
                                    (seq.tail(), "enter_driver", "leave_driver",
                                     "logical_volume_number", variant-1));
                break;
            case 'i': seq.addModule(new LatencyGroupByAccessNN
                                    (seq.tail(), "enter_driver", "leave_driver",
                                     "logical_volume_number", variant-1));
                break;
            case 'j':
                seq.addModule(new LatencyGroupByIntNN(seq.tail(), start_field, 
                                                      end_field, groupby_field));
                break;
            default:
                FATAL_ERROR("?");
        }
    }

    for (int i=optind; i<argc; ++i) {
        source->addSource(argv[i]);
    }
    seq.getAndDelete();
    
    RowAnalysisModule::printAllResults(seq,1);

    printf("extents: %.2f MB -> %.2f MB\n",
           (double)(source->total_compressed_bytes)/(1024.0*1024),
           (double)(source->total_uncompressed_bytes)/(1024.0*1024));
    printf("                   common\n");
    printf("MB compressed:   %8.2f\n",
           (double)source->total_compressed_bytes/(1024.0*1024));
    printf("MB uncompressed: %8.2f\n",
           (double)source->total_uncompressed_bytes/(1024.0*1024));
    printf("wait fraction :  %8.2f\n",
           source->waitFraction());
    
    return 0;
}
