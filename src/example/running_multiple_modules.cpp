// -*-C++-*-
/*
   (c) Copyright 2008 Harvey Mudd College

   See the file named COPYING for license details
*/

#include <boost/program_options.hpp>
#include <boost/foreach.hpp>

#include <DataSeries/RowAnalysisModule.hpp>
#include <DataSeries/DataSeriesFile.hpp>
#include <DataSeries/SequenceModule.hpp>
#include <DataSeries/TypeIndexModule.hpp>

/*  These two modules are minimal since they are not the point
    of this example.  For a better example of defining processing
    modules, see processing.cpp */

class Module1 : public RowAnalysisModule {
public:
    Module1(DataSeriesModule& source) : RowAnalysisModule(source), count(0) {}
    virtual void processRow() {
        ++count;
    }
    virtual void printResult() {
        std::cout << "Module1 processed " << count << "rows\n";
    }
private:
    int count;
};

class Module2 : public RowAnalysisModule {
public:
    Module2(DataSeriesModule& source) : RowAnalysisModule(source), count(0) {}
    virtual void processRow() {
        ++count;
    }
    virtual void printResult() {
        std::cout << "Module2 processed " << count << "rows\n";
    }
private:
    int count;
};

int main(int argc, char *argv[]) {

    boost::program_options::options_description options("Allowed Options");
    options.add_options()
        ("input-file", boost::program_options::value<std::vector<std::string> >(), "DataSeries files to process")
        ("module1", "Run Module1 analysis")
        ("module2", "Run module2 analysis");
    boost::program_options::positional_options_description positional;
    positional.add("input-file", -1);

    boost::program_options::variables_map command_line;

    try {
        boost::program_options::store(boost::program_options::command_line_parser(argc, argv).
              options(options).positional(positional).run(), command_line);
    } catch(boost::program_options::error& e) {
        std::cerr << e.what() << std::endl;
        std::cerr << options;
        return(2);
    }
    boost::program_options::notify(command_line);

    std::auto_ptr<TypeIndexModule> source(new TypeIndexModule("MyExtent"));
    BOOST_FOREACH(const std::string& source_file, command_line["input-file"].as<std::vector<std::string> >()) {
        source->addSource(source_file);
    }

    SequenceModule all_modules(source.release());

    if(command_line.count("module1")) {
        all_modules.addModule(new Module1(all_modules.tail()));
    }

    if(command_line.count("module2")) {
        all_modules.addModule(new Module2(all_modules.tail()));
    }
    
    all_modules.getAndDelete();
    RowAnalysisModule::printAllResults(all_modules);
}
