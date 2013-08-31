#include <iostream>
#include <DataSeries/TypeFilterModule.hpp>
#include <DataSeries/RowAnalysisModule.hpp>

class Counter : public RowAnalysisModule {
    uint64_t count;

  public:
    Counter(DataSeriesModule &source)
    : RowAnalysisModule(source, ExtentSeries::typeLoose),
      count(0)
    { }

    virtual void
    processRow()
    {
        count++;
    }

    uint64_t getCount() {
        return count;
    }
};

class PostfixFilter {
  public:
    PostfixFilter(const std::string &p) : postfix(p) { }

    bool operator()(const std::string &type) {
        return (type.substr(type.size() - postfix.size(),
                            postfix.size()) == postfix);
    }

  private:
    std::string postfix;
};


int main(int argc, char *argv[]) {
    PrefixFilter filter1("Trace::NFS");
    PrefixFilterModule test1(filter1);
    test1.addSource(argv[1]);
    Counter count1(test1);
    count1.getAndDeleteShared();
    std::cout << count1.getCount() << std::endl;

    PrefixFilter filter2("Trace::NFS::common");
    PrefixFilterModule test2(filter2);
    test2.addSource(argv[1]);
    Counter count2(test2);
    count2.getAndDeleteShared();
    std::cout << count2.getCount() << std::endl;

    PostfixFilter filter3("common");
    TypeFilterModule<PostfixFilter> test3(filter3);
    test3.addSource(argv[1]);
    Counter count3(test3);
    count3.getAndDeleteShared();
    std::cout << count3.getCount() << std::endl;
}
