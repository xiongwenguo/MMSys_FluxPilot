// Copyright (c) 2023. ByteDance Inc. All rights reserved.

#include "spdlog/spdlog.h"
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "MPD.h"
#include "configjson.hpp"
#include "demo/demotransportcontroller.hpp"
#include "playerevent.h"
#include <iostream>


int main(int argc, char** argv)
{
    ///////////////  import upside nodes from a json file ///////////////
    if (argc != 2)
    {
        std::cerr << " Need Json file" << std::endl;
        return -1;
    }

    ///////////////Set loggers///////////////
    /// check spdlog's manuel for more detail
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
    // uncomment the line below to write log to file, note: the log file size will is nearly same as the resource file
    //sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_mt>("logfile.txt"));
    auto combined_logger = std::make_shared<spdlog::logger>("mpdlogger", begin(sinks), end(sinks));
    //combined_logger->set_pattern("[%D %H:%M:%S.%e][%s %!: %#] %v");
    combined_logger->set_pattern("[%D %H:%M:%S.%e][%l][%s:%# %!()]%v");
    combined_logger->set_level(spdlog::level::debug);
    combined_logger->flush_on(spdlog::level::debug);
    spdlog::set_default_logger(combined_logger);

    ////////////////////parse initiate parameters//////////////////////////////
    std::string jsonpath(argv[1]);
    std::cout << "Json file :" << jsonpath << std::endl;
    DownNodeConfig downNodeConfig;
    if(!MPD::ParseDownloadJson(jsonpath,downNodeConfig))
    {
        spdlog::error("Parse Download Config Failed. Exit");
        return -1;
    }


    ///////////////Create a Transport Module Setting///////////////
    std::shared_ptr<TransportModuleSettings> myTransportModuleSettings = std::make_shared<TransportModuleSettings>();

    // create your TransportCtlConfig class here. The parameters inside this class will be passed to
    // your TransportController.

    std::shared_ptr<DemoTransportCtlConfig> myTransportCtlConfig = std::make_shared<DemoTransportCtlConfig>();
    // these values will be passed to demo transport module
    myTransportCtlConfig->minWnd = 1;
    myTransportCtlConfig->maxWnd = 64;

    // Create your TransportCtlFactory
    std::shared_ptr<DemoTransportCtlFactory> myTransportFactory = std::make_shared<DemoTransportCtlFactory>();

    // assemble transport controller config and transport controller factory together
    myTransportModuleSettings->transportCtlConfig = myTransportCtlConfig;
    myTransportModuleSettings->transportCtlFactory = myTransportFactory;

    /////////////////////////////// Now Set Up Dummy Player ///////////////

    // you may change or extend this class to receive player event
    auto myPlayerEventHandler = std::make_shared<PlayerEventHandler>();

    /// step 1:
    MPD::CreatePlayer();

    ///step 2:
    PlayMetaConfig myPlayConfig;
    myPlayConfig.filelengthinbyte = 10*1024*1024;
    MPD::InitPlayer(myPlayConfig,MPD::SDKLogLevel::DEBUG,myPlayerEventHandler);

    ///step 3:
    // register transport module setting
    MPD::SetTransportModuleSettings(myTransportModuleSettings);

    ///step 4:
    //start the player
    MPD::StartPlayer();

    ///step 5:
    // start the download process in 2 seconds after player is started.
    // we need some time to establish the session
    MPD::StartDownload(2,downNodeConfig);

    /// step 6:
    // start the whole testing
    MPD::StartTesting();

    /// step 7:
    MPD::WaitForFinished();

    spdlog::info(" test finished");

    return 0;
}