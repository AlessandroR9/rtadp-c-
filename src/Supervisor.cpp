// Copyright (C) 2024 INAF
// This software is distributed under the terms of the BSD-3-Clause license
//
// Authors:
//
//    Andrea Bulgarelli <andrea.bulgarelli@inaf.it>
//

#include "Supervisor.h"

Supervisor* Supervisor::instance = nullptr;

Supervisor::Supervisor(std::string config_file, std::string name)
    : name(name), continueall(true), config_manager(nullptr), manager_num_workers(0) {
    Supervisor::set_instance(this);  // Set the current instance
    load_configuration(config_file, name);
    fullname = name;
    globalname = "Supervisor-" + name;

    // Set up logging
    std::string log_file = config["logs_path"].get<std::string>() + "/" + globalname + ".log";
    logger = new WorkerLogger("worker_logger", log_file, spdlog::level::debug);

    pid = getpid();
    context = zmq::context_t(1);

    try {
        // Retrieve and log configuration
        processingtype = config["processing_type"].get<std::string>();
        dataflowtype = config["dataflow_type"].get<std::string>();
        std::cout << "\n\n\n dataflowtype: " << dataflowtype << std::endl;
        datasockettype = config["datasocket_type"].get<std::string>();

        std::cout << "Supervisor: " << globalname << " / " << dataflowtype << " / " 
                  << processingtype << " / " << datasockettype << std::endl;
        logger->system("Supervisor: " + globalname + " / " + dataflowtype + " / " 
                       + processingtype + " / " + datasockettype, globalname);

        // Set up data sockets based on configuration
        if (datasockettype == "pushpull") {
            socket_lp_data = new zmq::socket_t(context, ZMQ_PULL);
            socket_lp_data->bind(config["data_lp_socket"].get<std::string>());

            socket_hp_data = new zmq::socket_t(context, ZMQ_PULL);
            socket_hp_data->bind(config["data_hp_socket"].get<std::string>());
        } else if (datasockettype == "pubsub") {
            socket_lp_data = new zmq::socket_t(context, ZMQ_SUB);
            socket_lp_data->connect(config["data_lp_socket"].get<std::string>());
            socket_lp_data->setsockopt(ZMQ_SUBSCRIBE, "", 0);

            socket_hp_data = new zmq::socket_t(context, ZMQ_SUB);
            socket_hp_data->connect(config["data_hp_socket"].get<std::string>());
            socket_hp_data->setsockopt(ZMQ_SUBSCRIBE, "", 0);
        } else if (datasockettype == "custom") {
            logger->system("Supervisor started with custom data receiver", globalname);
        } else {
            throw std::invalid_argument("Config file: datasockettype must be pushpull or pubsub");
        }

        // Set up command and monitoring sockets
        socket_command = new zmq::socket_t(context, ZMQ_SUB);
        socket_command->connect(config["command_socket"].get<std::string>());
        socket_command->setsockopt(ZMQ_SUBSCRIBE, "", 0);

        socket_monitoring = new zmq::socket_t(context, ZMQ_PUSH);
        socket_monitoring->connect(config["monitoring_socket"].get<std::string>());

        socket_lp_result.resize(100, nullptr);
        socket_hp_result.resize(100, nullptr);
    } catch (const std::exception &e) {
        // Handle any other unexpected exceptions
        std::cerr << "ERROR: An unexpected error occurred: " << e.what() << std::endl;
        logger->warning("ERROR: An unexpected error occurred: " + std::string(e.what()), globalname);
        exit(1);
    }

    manager_workers = std::vector<WorkerManager*>();
    processdata = 0;
    stopdata = true;

    // Set up signal handlers
    try {
        signal(SIGTERM, handle_signals);
        signal(SIGINT, handle_signals);
    } catch (const std::exception &e) {
        std::cerr << "WARNING! Signal only works in main thread. It is not possible to set up signal handlers!" << std::endl;
        logger->warning("WARNING! Signal only works in main thread. It is not possible to set up signal handlers!", globalname);
    }

    status = "Initialised";
    send_info(1, status, fullname, 1, "Low");

    std::cout << globalname << " started" << std::endl;
    logger->system(globalname + " started", globalname);
}

// Destructor to clean up resources
Supervisor::~Supervisor() {
    delete socket_lp_data;
    delete socket_hp_data;
    delete socket_command;
    delete socket_monitoring;
    delete logger;
}


// Static method to set the current instance
void Supervisor::set_instance(Supervisor *instance) {
    Supervisor::instance = instance;
}

// Static method to get the current instance
Supervisor* Supervisor::get_instance() {
    return Supervisor::instance;
}


std::vector<std::string> Supervisor::getNameWorkers() const {
    return worker_names;
}

// Load configuration from the specified file and name
void Supervisor::load_configuration(const std::string &config_file, const std::string &name) {
    config_manager = new ConfigurationManager(config_file);
    config = config_manager->get_configuration(name);
    std::cout << config << std::endl;

    // Extract values from the tuple returned by get_workers_config
    auto workers_config = config_manager->get_workers_config(name);
    manager_result_sockets_type = std::get<0>(workers_config)[0]; // assuming single value
    manager_result_dataflow_type = std::get<1>(workers_config)[0]; // assuming single value
    manager_result_lp_sockets = std::get<2>(workers_config);
    manager_result_hp_sockets = std::get<3>(workers_config);
    manager_num_workers = std::get<4>(workers_config)[0]; // assuming single value
    workername = std::get<5>(workers_config)[0]; // assuming single value
    name_workers = std::get<6>(workers_config);
}


// Start service threads for data handling
void Supervisor::start_service_threads() {
    if (dataflowtype == "binary") {
        lp_data_thread = std::thread(&Supervisor::listen_for_lp_data, this);
        hp_data_thread = std::thread(&Supervisor::listen_for_hp_data, this);
    } else if (dataflowtype == "filename") {
        lp_data_thread = std::thread(&Supervisor::listen_for_lp_file, this);
        hp_data_thread = std::thread(&Supervisor::listen_for_hp_file, this);
    } else if (dataflowtype == "string") {
        lp_data_thread = std::thread(&Supervisor::listen_for_lp_string, this);
        hp_data_thread = std::thread(&Supervisor::listen_for_hp_string, this);
    }

    result_thread = std::thread(&Supervisor::listen_for_result, this);
}


// Set up result channel for a given WorkerManager
void Supervisor::setup_result_channel(WorkerManager *manager, int indexmanager) {
    socket_lp_result[indexmanager] = nullptr;
    socket_hp_result[indexmanager] = nullptr;
    //context = zmq::context_t(1);
    if (manager->get_result_lp_socket() != "none") {
        if (manager->get_result_socket_type() == "pushpull") {
            socket_lp_result[indexmanager] = new zmq::socket_t(context, ZMQ_PUSH);
            socket_lp_result[indexmanager]->connect(manager->get_result_lp_socket());
            std::cout << "---result lp socket pushpull " << manager->get_globalname() << " " << manager->get_result_lp_socket() << std::endl;
            logger->system("---result lp socket pushpull " + manager->get_globalname() + " " + manager->get_result_lp_socket(), globalname);
        } else if (manager->get_result_socket_type() == "pubsub") {
            socket_lp_result[indexmanager] = new zmq::socket_t(context, ZMQ_PUB);
            socket_lp_result[indexmanager]->bind(manager->get_result_lp_socket());
            std::cout << "---result lp socket pushpull " << manager->get_globalname() << " " << manager->get_result_lp_socket() << std::endl;
            logger->system("---result lp socket pushpull " + manager->get_globalname() + " " + manager->get_result_lp_socket(), globalname);
        }
    }

    if (manager->get_result_hp_socket() != "none") {
        if (manager->get_result_socket_type() == "pushpull") {
            socket_hp_result[indexmanager] = new zmq::socket_t(context, ZMQ_PUSH);
            socket_hp_result[indexmanager]->connect(manager->get_result_hp_socket());
            std::cout << "---result hp socket pushpull " << manager->get_globalname() << " " << manager->get_result_hp_socket() << std::endl;
            logger->system("---result hp socket pushpull " + manager->get_globalname() + " " + manager->get_result_hp_socket(), globalname);
        } else if (manager->get_result_socket_type() == "pubsub") {
            socket_hp_result[indexmanager] = new zmq::socket_t(context, ZMQ_PUB);
            socket_hp_result[indexmanager]->bind(manager->get_result_hp_socket());
            std::cout << "---result hp socket pushpull " << manager->get_globalname() << " " << manager->get_result_hp_socket() << std::endl;
            logger->system("---result hp socket pushpull " + manager->get_globalname() + " " + manager->get_result_hp_socket(), globalname);
        }
    }
}



// Start managers
void Supervisor::start_managers() {
    int indexmanager = 0;
    WorkerManager *manager = new WorkerManager(indexmanager, this, "Generic");
    setup_result_channel(manager, indexmanager);
    manager->run();
    manager_workers.push_back(manager);
    std::cout << "BASE SUP manager started. man lenght: " << manager_workers.size() << std::endl;
}

// Start workers
void Supervisor::start_workers() {
    int indexmanager = 0;
    for (auto &manager : manager_workers) {
        manager->start_worker_threads(manager_num_workers);
        std::cout << "SUP start_worker_threads" << std::endl;
        indexmanager++;
    }
}

// Start Supervisor operation
void Supervisor::start() {
    start_service_threads();
    start_managers();
    start_workers();

    status = "Waiting";
    send_info(1, status, fullname, 1, "Low");

    try {
        while (continueall) {
            listen_for_commands();
            std::this_thread::sleep_for(std::chrono::seconds(1)); // To avoid 100% CPU
        }
    } catch (const std::exception &e) {
        std::cerr << "Exception caught: " << e.what() << std::endl;
        command_shutdown();
    }
}

// Static function to handle signals
void Supervisor::handle_signals(int signum) {
    Supervisor* instance = Supervisor::get_instance();
    if (instance) {
        if (signum == SIGTERM) {
            std::cerr << "SIGTERM received. Terminating with cleaned shutdown." << std::endl;
            instance->logger->system("SIGTERM received. Terminating with cleaned shutdown", instance->globalname);
            instance->command_cleanedshutdown();
        } else if (signum == SIGINT) {
            std::cerr << "SIGINT received. Terminating with shutdown." << std::endl;
            instance->logger->system("SIGINT received. Terminating with shutdown", instance->globalname);
            instance->command_shutdown();
        } else {
            std::cerr << "Received signal " << signum << ". Terminating." << std::endl;
            instance->logger->system("Received signal " + std::to_string(signum) + ". Terminating", instance->globalname);
            instance->command_shutdown();
        }
    }
}

// Listen for result data
void Supervisor::listen_for_result() {
    while (continueall) {
        int indexmanager = 0;
    
        for (auto &manager : manager_workers) {
            while(manager == nullptr){
            std::this_thread::sleep_for(std::chrono::seconds(1));  // Sleep for 1 second
            std::cout << "waiting for manager workers for listening results" << std::endl;
        }
            send_result(manager, indexmanager);
            indexmanager++;
        }
    }
    std::cout << "End listen_for_result" << std::endl;
    logger->system("End listen_for_result", globalname);
}

// Send result data
void Supervisor::send_result(WorkerManager *manager, int indexmanager) {
    if (manager->getResultLpQueue()->size() == 0 && manager->getResultHpQueue()->size() == 0) {
        return;
    }

    json data;
    int channel = -1;
    try {
        channel = 1;
        data = manager->getResultHpQueue()->front();
        manager->getResultHpQueue()->pop();
    } catch (const std::exception &e) {
        try {
            channel = 0;
            data = manager->getResultLpQueue()->front();
            manager->getResultLpQueue()->pop();
        } catch (const std::exception &e) {
            return;
        }
    }

    if (channel == 0) {
        if (manager->get_result_lp_socket() == "none") {
            return;
        }
        if (manager->get_result_dataflow_type() == "string" || manager->get_result_dataflow_type() == "filename") {
            try {
                std::string data_str = data.get<std::string>();
                socket_lp_result[indexmanager]->send(zmq::buffer(data_str));
            } catch (const std::exception &e) {
                std::cerr << "ERROR: data not in string format to be sent to: " << e.what() << std::endl;
                logger->error("ERROR: data not in string format to be sent to: " + std::string(e.what()), globalname);
            }
        } else if (manager->get_result_dataflow_type() == "binary") {
            try {
                socket_lp_result[indexmanager]->send(zmq::buffer(data.dump()));
            } catch (const std::exception &e) {
                std::cerr << "ERROR: data not in binary format to be sent to socket_result: " << e.what() << std::endl;
                logger->error("ERROR: data not in binary format to be sent to socket_result: " + std::string(e.what()), globalname);
            }
        }
    }

    if (channel == 1) {
        if (manager->get_result_hp_socket() == "none") {
            return;
        }
        if (manager->get_result_dataflow_type() == "string" || manager->get_result_dataflow_type() == "filename") {
            try {
                std::string data_str = data.get<std::string>();
                socket_hp_result[indexmanager]->send(zmq::buffer(data_str));
            } catch (const std::exception &e) {
                std::cerr << "ERROR: data not in string format to be sent to: " << e.what() << std::endl;
                logger->error("ERROR: data not in string format to be sent to: " + std::string(e.what()), globalname);
            }
        } else if (manager->get_result_dataflow_type() == "binary") {
            try {
                socket_hp_result[indexmanager]->send(zmq::buffer(data.dump()));
            } catch (const std::exception &e) {
                std::cerr << "ERROR: data not in binary format to be sent to socket_result: " << e.what() << std::endl;
                logger->error("ERROR: data not in binary format to be sent to socket_result: " + std::string(e.what()), globalname);
            }
        }
    }
}

// Listen for low priority data
void Supervisor::listen_for_lp_data() {
    while (continueall) {
        if (!stopdata) {
            zmq::message_t data;
            socket_lp_data->recv(data);
            for (auto &manager : manager_workers) {
                json decodeddata = json::parse(data.to_string());
                manager->getLowPriorityQueue()->push(decodeddata);
            }
        }
    }
    std::cout << "End listen_for_lp_data" << std::endl;
    logger->system("End listen_for_lp_data", globalname);
}

// Listen for high priority data
void Supervisor::listen_for_hp_data() {
    while (continueall) {
        if (!stopdata) {
            zmq::message_t data;
            socket_hp_data->recv(data);
            for (auto &manager : manager_workers) {
                json decodeddata = json::parse(data.to_string());
                manager->getHighPriorityQueue()->push(decodeddata);
            }
        }
    }
    std::cout << "End listen_for_hp_data" << std::endl;
    logger->system("End listen_for_hp_data", globalname);
}

// Listen for low priority strings
void Supervisor::listen_for_lp_string() {
    while (continueall) {
        if (!stopdata) {
            zmq::message_t data;
            socket_lp_data->recv(data);
            std::string data_str(static_cast<char*>(data.data()), data.size());
            for (auto &manager : manager_workers) {
                manager->getLowPriorityQueue()->push(data_str);
            }
        }
    }
    std::cout << "End listen_for_lp_string" << std::endl;
    logger->system("End listen_for_lp_string", globalname);
}

// Listen for high priority strings
void Supervisor::listen_for_hp_string() {
    while (continueall) {
        if (!stopdata) {
            zmq::message_t data;
            socket_hp_data->recv(data);
            std::string data_str(static_cast<char*>(data.data()), data.size());
            for (auto &manager : manager_workers) {
                manager->getHighPriorityQueue()->push(data_str);
            }
        }
    }
    std::cout << "End listen_for_hp_string" << std::endl;
    logger->system("End listen_for_hp_string", globalname);
}

// Listen for low priority files
void Supervisor::listen_for_lp_file() {
    while (continueall) {
        if (!stopdata) {
            zmq::message_t filename_msg;
            socket_lp_data->recv(filename_msg);
            std::string filename(static_cast<char*>(filename_msg.data()), filename_msg.size());
            for (auto &manager : manager_workers) {
                auto [data, size] = open_file(filename);
                for (int i = 0; i < size; i++) {
                    manager->getLowPriorityQueue()->push(data[i]);
                }
            }
        }
    }
    std::cout << "End listen_for_lp_file" << std::endl;
    logger->system("End listen_for_lp_file", globalname);
}



std::pair<std::vector<json>, int> Supervisor::open_file(const std::string &filename) {
    std::vector<json> data;  // Vector to store parsed JSON objects
    int size = 0;

    std::ifstream file(filename); // Open the file for reading
    if (!file.is_open()) {
        std::cerr << "Unable to open file: " << filename << std::endl;
        logger->error("Unable to open file: " + filename, globalname);
        return {data, size};  // Return empty vector and size 0 if the file cannot be opened
    }

    try {
        std::string line;
        while (std::getline(file, line)) { // Read the file line-by-line
            if (!line.empty()) { // Only attempt to parse non-empty lines
                json jsonData = json::parse(line); // Parse the line as JSON
                data.push_back(jsonData); // Add the parsed JSON object to the vector
                size++;
            }
        }
    } catch (const std::exception &e) {
        std::cerr << "Error while reading file: " << e.what() << std::endl;
        logger->error("Error while reading file: " + std::string(e.what()), globalname);
    }

    file.close(); // Close the file after reading
    return {data, size}; // Return the vector and the size of the data read
}

// Listen for high priority files
void Supervisor::listen_for_hp_file() {
    while (continueall) {
        if (!stopdata) {
            zmq::message_t filename_msg;
            socket_hp_data->recv(filename_msg);
            std::string filename(static_cast<char*>(filename_msg.data()), filename_msg.size());
            for (auto &manager : manager_workers) {
                auto [data, size] = open_file(filename);
                for (int i = 0; i < size; i++) {
                    manager->getHighPriorityQueue()->push(data[i]);
                }
            }
        }
    }
    std::cout << "End listen_for_hp_file" << std::endl;
    logger->system("End listen_for_hp_file", globalname);
}

// Listen for commands
void Supervisor::listen_for_commands() {
    while (continueall) {
        std::cout << "Waiting for commands..." << std::endl;
        logger->system("Waiting for commands...", globalname);

        zmq::message_t command_msg;
        socket_command->recv(command_msg);
        std::string command_str(static_cast<char*>(command_msg.data()), command_msg.size());
        json command = json::parse(command_str);
        process_command(command);
    }
    std::cout << "End listen_for_commands" << std::endl;
    logger->system("End listen_for_commands", globalname);
}

// Shutdown command
void Supervisor::command_shutdown() {
    status = "Shutdown";
    stop_all(false);
}

// Cleaned shutdown command
void Supervisor::command_cleanedshutdown() {
    if (status == "Processing") {
        status = "EndingProcessing";
        command_stopdata();
        for (auto &manager : manager_workers) {
            std::cout << "Trying to stop " << manager->get_globalname() << "..." << std::endl;
            logger->system("Trying to stop " + manager->get_globalname() + "...", globalname);
            while (manager->getLowPriorityQueue()->size() != 0 || manager->getHighPriorityQueue()->size() != 0) {
                std::cout << "Queues data of manager " << manager->get_globalname() << " have size " 
                          << manager->getLowPriorityQueue()->size() << " " << manager->getHighPriorityQueue()->size() << std::endl;
                logger->system("Queues data of manager " + manager->get_globalname() + " have size " 
                               + std::to_string(manager->getLowPriorityQueue()->size()) + " " 
                               + std::to_string(manager->getHighPriorityQueue()->size()), globalname);
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            while (manager->getResultLpQueue()->size() != 0 || manager->getResultHpQueue()->size() != 0) {
                std::cout << "Queues result of manager " << manager->get_globalname() << " have size " 
                          << manager->getResultLpQueue()->size() << " " << manager->getResultHpQueue()->size() << std::endl;
                logger->system("Queues result of manager " + manager->get_globalname() + " have size " 
                               + std::to_string(manager->getResultLpQueue()->size()) + " " 
                               + std::to_string(manager->getResultHpQueue()->size()), globalname);
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
        }
    } else {
        std::cerr << "WARNING! Not in Processing state for a cleaned shutdown. Force the shutdown." << std::endl;
        logger->warning("WARNING! Not in Processing state for a cleaned shutdown. Force the shutdown.", globalname);
    }

    status = "Shutdown";
    stop_all(false);
}

// Reset command
void Supervisor::command_reset() {
    if (status == "Processing" || status == "Waiting") {
        command_stop();
        for (auto &manager : manager_workers) {
            std::cout << "Trying to reset " << manager->get_globalname() << "..." << std::endl;
            logger->system("Trying to reset " + manager->get_globalname() + "...", globalname);
            manager->clean_queue();
            std::cout << "Queues of manager " << manager->get_globalname() << " have size " 
                      << manager->getLowPriorityQueue()->size() << " " << manager->getHighPriorityQueue()->size() << " " 
                      << manager->getResultLpQueue()->size() << " " << manager->getResultHpQueue()->size() << std::endl;
            logger->system("Queues of manager " + manager->get_globalname() + " have size " 
                           + std::to_string(manager->getLowPriorityQueue()->size()) + " " 
                           + std::to_string(manager->getHighPriorityQueue()->size()) + " " 
                           + std::to_string(manager->getResultLpQueue()->size()) + " " 
                           + std::to_string(manager->getResultHpQueue()->size()), globalname);
        }
        status = "Waiting";
        send_info(1, status, fullname, 1, "Low");
    }
}

// Start command
void Supervisor::command_start() {
    std::cout << "COMMAND START" << std::endl;
    command_startprocessing();
    command_startdata();
}

// Stop command
void Supervisor::command_stop() {
    command_stopdata();
    command_stopprocessing();
}

// Start processing command
void Supervisor::command_startprocessing() {
    status = "Processing";
    std::cout << "CCCCC" << std::endl;
    send_info(1, status, fullname, 1, "Low");
    for (auto &manager : manager_workers) {
        manager->set_processdata(1);
    }
}

// Stop processing command
void Supervisor::command_stopprocessing() {
    status = "Waiting";
    send_info(1, status, fullname, 1, "Low");
    for (auto &manager : manager_workers) {
        manager->set_processdata(0);
    }
}

// Start data command
void Supervisor::command_startdata() {
    stopdata = false;
    for (auto &manager : manager_workers) {
        manager->set_stopdata(false);
    }
}

// Stop data command
void Supervisor::command_stopdata() {
    stopdata = true;
    for (auto &manager : manager_workers) {
        manager->set_stopdata(true);
    }
}

// Process received command
void Supervisor::process_command(const json &command) {
    int type_value = command["header"]["type"].get<int>();
    std::string subtype_value = command["header"]["subtype"].get<std::string>();
    std::string pidtarget = command["header"]["pidtarget"].get<std::string>();
    std::string pidsource = command["header"]["pidsource"].get<std::string>();

    if (type_value == 0) { // command
        if (pidtarget == name || pidtarget == "all" || pidtarget == "*") {
            std::cout << "Received command: " << command << std::endl;
            if (subtype_value == "shutdown") {
                command_shutdown();
            } else if (subtype_value == "cleanedshutdown") {
                command_cleanedshutdown();
            } else if (subtype_value == "getstatus") {
                for (auto &manager : manager_workers) {
                    manager->getMonitoringThread()->sendto(pidsource);
                }
            } else if (subtype_value == "start") {
                command_start();
            } else if (subtype_value == "stop") {
                command_stop();
            } else if (subtype_value == "startprocessing") {
                command_startprocessing();
            } else if (subtype_value == "stopprocessing") {
                command_stopprocessing();
            } else if (subtype_value == "reset") {
                command_reset();
            } else if (subtype_value == "stopdata") {
                command_stopdata();
            } else if (subtype_value == "startdata") {
                command_startdata();
            }
        }
    } else if (type_value == 3) { // config
        for (auto &manager : manager_workers) {
            manager->configworkers(command);
        }
    }
}

// Send alarm message
void Supervisor::send_alarm(int level, const std::string &message, const std::string &pidsource, int code, const std::string &priority) {
    json msg;
    msg["header"]["type"] = 2;
    msg["header"]["subtype"] = "alarm";
    msg["header"]["time"] = static_cast<double>(time(nullptr));
    msg["header"]["pidsource"] = pidsource;
    msg["header"]["pidtarget"] = "*";
    msg["header"]["priority"] = priority;
    msg["body"]["level"] = level;
    msg["body"]["code"] = code;
    msg["body"]["message"] = message;
    socket_monitoring->send(zmq::buffer(msg.dump()));
}

// Send log message
void Supervisor::send_log(int level, const std::string &message, const std::string &pidsource, int code, const std::string &priority) {
    json msg;
    msg["header"]["type"] = 4;
    msg["header"]["subtype"] = "log";
    msg["header"]["time"] = static_cast<double>(time(nullptr));
    msg["header"]["pidsource"] = pidsource;
    msg["header"]["pidtarget"] = "*";
    msg["header"]["priority"] = priority;
    msg["body"]["level"] = level;
    msg["body"]["code"] = code;
    msg["body"]["message"] = message;
    socket_monitoring->send(zmq::buffer(msg.dump()));
}

// Send info message
void Supervisor::send_info(int level, const std::string &message, const std::string &pidsource, int code, const std::string &priority) {
    json msg;
    msg["header"]["type"] = 5;
    msg["header"]["subtype"] = "info";
    msg["header"]["time"] = static_cast<double>(time(nullptr));
    msg["header"]["pidsource"] = pidsource;
    msg["header"]["pidtarget"] = "*";
    msg["header"]["priority"] = priority;
    msg["body"]["level"] = level;
    msg["body"]["code"] = code;
    msg["body"]["message"] = message;
    socket_monitoring->send(zmq::buffer(msg.dump()));
}

// Stop all threads and processes
void Supervisor::stop_all(bool fast) {
    std::cout << "Stopping all workers and managers..." << std::endl;
    logger->system("Stopping all workers and managers...", globalname);

    command_stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    for (auto &manager : manager_workers) {
        manager->stop(fast);
    }

    continueall = false;

    std::cout << "All Supervisor workers and managers and internal threads terminated." << std::endl;
    logger->system("All Supervisor workers and managers and internal threads terminated.", globalname);
}